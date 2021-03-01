// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// PipelinedWindowExec is the executor for window functions.
type PipelinedWindowExec struct {
	baseExecutor

	groupChecker *vecGroupChecker
	// childResult stores the child chunk
	childResult *chunk.Chunk
	// executed indicates the child executor is drained or something unexpected happened.
	executed bool
	// resultChunks stores the chunks to return
	resultChunks []*chunk.Chunk
	// remainingRowsInChunk indicates how many rows the resultChunks[i] is not prepared.
	remainingRowsInChunk []int

	numWindowFuncs int
	processor      windowProcessor
	p              processor // TODO(zhifeng): you don't need a processor, just make it into the executor
	rows           []chunk.Row
	consumed       bool
	newPartition   bool
}

// Close implements the Executor Close interface.
func (e *PipelinedWindowExec) Close() error {
	return errors.Trace(e.baseExecutor.Close())
}

// Open implements the Executor Open interface
func (e *PipelinedWindowExec) Open(ctx context.Context) error {
	e.consumed = true
	return e.baseExecutor.Open(ctx)
}

// Next implements the Executor Next interface.
func (e *PipelinedWindowExec) Next(ctx context.Context, chk *chunk.Chunk) (err error) {
	chk.Reset()
	var wantMore int64
	for {
		// e.p is ready to produce data, we use wantMore here to avoid meaningless produce when the whole frame is required
		for wantMore == 0 {
			produced, err := e.p.produce(e.ctx, chk)
			if err != nil {
				return err
			}
			if produced == 0 || e.p.available == 0 {
				// either the chunk is full or no more available rows to produce
				break
			}
		}
		if chk.IsFull() {
			break
		}
		// reaching here, e.p.available must be 0, because if produced == 0 and available is not 0, it must be
		// that the chk is full, and it will break in the above branch
		if e.consumed {
			err = e.getRowsInPartition(ctx)
			if err != nil {
				return err
			}
			if e.newPartition {
				e.p.finish()
				wantMore = 0
				continue
			}
		}
		// reaching here, e.consumed must be false. Initially, e.consumed is default to true, and the above branch will be
		// entered, and e.consumed will be false. If it enters a new partition, the e.consumed will be kept as false before
		// coming here.
		if e.newPartition {
			e.p.reset()
			if len(e.rows) == 0 {
				// no more data
				break
			}
		}
		wantMore, err = e.p.consume(e.ctx, e.rows)
		e.consumed = true // TODO(zhifeng): move this into consume() after absorbing p into e
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *PipelinedWindowExec) getRowsInPartition(ctx context.Context) (err error) {
	e.newPartition = false
	e.rows = e.rows[0:]

	if e.groupChecker.isExhausted() {
		var drained, samePartition bool
		drained, err = e.fetchChild(ctx)
		if err != nil {
			err = errors.Trace(err)
		}
		if drained {
			e.newPartition = true
			return nil
		}
		samePartition, err = e.groupChecker.splitIntoGroups(e.childResult)
		e.newPartition = !samePartition
		if err != nil {
			return errors.Trace(err)
		}
	}
	begin, end := e.groupChecker.getNextGroup()
	for i := begin; i < end; i++ {
		e.rows = append(e.rows, e.childResult.GetRow(i))
	}
	e.consumed = false
	return
}

func (e *PipelinedWindowExec) preparedChunkAvailable() bool {
	return len(e.resultChunks) > 0 && e.remainingRowsInChunk[0] == 0
}

func (e *PipelinedWindowExec) fetchChild(ctx context.Context) (EOF bool, err error) {
	// TODO: reuse chunks
	childResult := newFirstChunk(e.children[0])
	err = Next(ctx, e.children[0], childResult)
	if err != nil {
		return false, errors.Trace(err)
	}
	// No more data.
	numRows := childResult.NumRows()
	if numRows == 0 {
		return true, nil
	}

	// TODO: reuse chunks
	resultChk := chunk.New(e.retFieldTypes, 0, numRows)
	err = e.copyChk(childResult, resultChk)
	if err != nil {
		return false, err
	}
	e.resultChunks = append(e.resultChunks, resultChk)
	e.remainingRowsInChunk = append(e.remainingRowsInChunk, numRows)

	e.childResult = childResult
	return false, nil
}

func (e *PipelinedWindowExec) copyChk(src, dst *chunk.Chunk) error {
	columns := e.Schema().Columns[:len(e.Schema().Columns)-e.numWindowFuncs]
	for i, col := range columns {
		if err := dst.MakeRefTo(i, src, col.Index); err != nil {
			return err
		}
	}
	return nil
}

type processor struct {
	windowFuncs        []aggfuncs.AggFunc
	slidingWindowFuncs []aggfuncs.SlidingWindowAggFunc
	partialResults     []aggfuncs.PartialResult
	start              *core.FrameBound
	end                *core.FrameBound
	curRowIdx          uint64
	// curStartRow and curEndRow defines the current frame range
	curStartRow uint64
	curEndRow   uint64
	// rows keeps rows starting from curStartRow, TODO(zhifeng): make it a queue
	rows         []chunk.Row
	whole        bool
	rowCnt       uint64
	isRangeFrame bool
	available    int64
}

func (p *processor) init() {
	slidingWindowAggFuncs := make([]aggfuncs.SlidingWindowAggFunc, len(p.windowFuncs))
	for i, windowFunc := range p.windowFuncs {
		if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
			slidingWindowAggFuncs[i] = slidingWindowAggFunc
		}
	}
}

// slide consumes more rows, and it returns the number of more rows needed in order to proceed, more = -1 means it can only
// process when the whole partition is consumed, more = 0 means it is ready now, more = n (n > 0), means it need at least n
// more rows to process.
func (p *processor) consume(ctx sessionctx.Context, rows []chunk.Row) (more int64, err error) {
	num := uint64(len(rows))
	p.rowCnt += num
	p.rows = append(p.rows, rows...)
	// the question is, rowNumber would have frame (of length 1) differently than other processor?
	// well, 1 processor only process 1 frame, you could test to see if rowNumber and sum over row frame will generate
	// 2 window function. but we could still support different frame here, it just adds complexity
	for i, wf := range p.windowFuncs {
		// TODO: add mem track later
		_, err = wf.UpdatePartialResult(ctx, rows, p.partialResults[i])
		if err != nil {
			return
		}
	}

	return
}

// finish is called upon a whole partition is consumed
func (p *processor) finish() {
	p.whole = true
}

// produce produces rows and append it to chk, return produced means number of rows appended into chunk, available means
// number of rows processed but not fetched
func (p *processor) produce(ctx sessionctx.Context, chk *chunk.Chunk) (produced int, err error) {
	for !chk.IsFull() && p.curRowIdx < p.rowCnt {

	}
	return
}

// reset resets the processor
func (p *processor) reset() {
	p.curRowIdx = 0
	p.rowCnt = 0
	p.whole = false
}
