// Copyright 2020 PingCAP, Inc.
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
	"strconv"
	"strings"

	//"fmt"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/bitmap"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
	//"github.com/pingcap/tidb/util/stringutil"
)

// WARNING: this executor is during its early experimental stage, DO NOT USE IN PRODUCTION
type MultwayHashJoinExecBuildSide struct {
	// buildSide
	buildSideExec     Executor
	buildSideEstCount float64
	buildKeys         []*expression.Column
	buildTypes        []*types.FieldType
	// outerFilter can be at buildSide or probeSide
	outerFilter       expression.CNFExprs
	rowContainer  *hashRowContainer



	// We build individual joiner for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joiners []joiner
	buildFinished chan error
}

// MultiwayHashJoinExec implements the multiway hash join algorithm.
// Currently we only experimentally support inner joins on same join keys across all joined multiple table.
type MultiwayHashJoinExec struct {
	baseExecutor

	probeSideExec     Executor
	probeKeys         []*expression.Column
	probeTypes        []*types.FieldType
	// outerFilter can only be at probeSide if we are running inner join
	outerFilter       expression.CNFExprs

	// buildSide
	numBuildExec	  int
	buildSideExec     []Executor
	buildSideEstCount []float64
	buildKeys         [][]*expression.Column
	buildTypes        [][]*types.FieldType
	rowContainer      []*hashRowContainer
	joinRetTypes      []



	// We build individual joiner for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joiners [][]joiner


	// concurrency is the number of partition, build and join workers.
	concurrency   uint
	buildFinished []chan error

	// closeCh add a lock for closing executor.
	closeCh      chan struct{}
	joinType     plannercore.JoinType
	requiredRows int64


	probeChkResourceCh chan *probeChkResource
	probeResultChs     []chan *chunk.Chunk
	joinChkResourceCh  []chan *chunk.Chunk
	joinResultCh       chan *hashjoinWorkerResult

	memTracker  *memory.Tracker // track memory usage.
	diskTracker *disk.Tracker   // track disk usage.

	outerMatchedStatus []*bitmap.ConcurrentBitmap
	useOuterToBuild    bool

	prepared    bool
	isOuterJoin bool

	// joinWorkerWaitGroup is for sync multiple join workers.
	joinWorkerWaitGroup sync.WaitGroup
	finished            atomic.Value
}
//
//// probeChkResource stores the result of the join probe side fetch worker,
//// `dest` is for Chunk reuse: after join workers process the probe side chunk which is read from `dest`,
//// they'll store the used chunk as `chk`, and then the probe side fetch worker will put new data into `chk` and write `chk` into dest.
//type probeChkResource struct {
//	chk  *chunk.Chunk
//	dest chan<- *chunk.Chunk
//}
//
//// hashjoinWorkerResult stores the result of join workers,
//// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
//// and push `chk` into `src` after processing, join worker goroutines get the empty chunk from `src`
//// and push new data into this chunk.
//type hashjoinWorkerResult struct {
//	chk *chunk.Chunk
//	err error
//	src chan<- *chunk.Chunk
//}

// Close implements the Executor Close interface.
func (e *MultiwayHashJoinExec) Close() error {
	close(e.closeCh)
	e.finished.Store(true)
	if e.prepared {
		if e.buildFinished != nil {
			for _, ch := range e.buildFinished {
				for range ch {
				}
			}
		}
		if e.joinResultCh != nil {
			for range e.joinResultCh {
			}
		}
		if e.probeChkResourceCh != nil {
			close(e.probeChkResourceCh)
			for range e.probeChkResourceCh {
			}
		}
		for i := range e.probeResultChs {
			for range e.probeResultChs[i] {
			}
		}
		for i := range e.joinChkResourceCh {
			close(e.joinChkResourceCh[i])
			for range e.joinChkResourceCh[i] {
			}
		}
		e.probeChkResourceCh = nil
		e.joinChkResourceCh = nil
		for _, rc := range e.rowContainer {
			terror.Call(rc.Close)
		}
	}

	if e.runtimeStats != nil {
		concurrency := cap(e.joiners)
		e.runtimeStats.SetConcurrencyInfo("Concurrency", concurrency)
		rcStats := make([]string, len(e.rowContainer))
		for i, rc := range e.rowContainer {
			rcStats = append(rcStats, strconv.Itoa(i) + "th build side: " + rc.stat.String())
		}
		e.runtimeStats.SetAdditionalInfo(strings.Join(rcStats, ";"))
	}
	err := e.baseExecutor.Close()
	return err
}

// Open implements the Executor Open interface.
func (e *MultiwayHashJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.numBuildExec = len(e.buildSideExec)
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.diskTracker = disk.NewTracker(e.id, -1)
	e.diskTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.DiskTracker)

	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.joinWorkerWaitGroup = sync.WaitGroup{}

	if e.probeTypes == nil {
		e.probeTypes = retTypes(e.probeSideExec)
	}
	if e.buildTypes == nil {
		for i, exec := range e.buildSideExec {
			e.buildTypes[i] = retTypes(exec)
		}
	}
	return nil
}

// hashjoinWorkerResult stores the result of join workers,
// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
// and push `chk` into `src` after processing, join worker goroutines get the empty chunk from `src`
// and push new data into this chunk.
type multiwayHashJoinWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

// fetchProbeSideChunks get chunks from fetches chunks from the big table in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join workers.
func (e *MultiwayHashJoinExec) fetchProbeSideChunks(ctx context.Context) {
	hasWaitedForBuild := false
	for {
		if e.finished.Load().(bool) {
			return
		}

		var probeSideResource *probeChkResource
		var ok bool
		select {
		case <-e.closeCh:
			return
		case probeSideResource, ok = <-e.probeChkResourceCh:
			if !ok {
				return
			}
		}
		probeSideResult := probeSideResource.chk
		if e.isOuterJoin {
			required := int(atomic.LoadInt64(&e.requiredRows))
			probeSideResult.SetRequiredRows(required, e.maxChunkSize)
		}
		err := Next(ctx, e.probeSideExec, probeSideResult)
		if err != nil {
			e.joinResultCh <- &hashjoinWorkerResult{
				err: err,
			}
			return
		}
		if !hasWaitedForBuild {
			if probeSideResult.NumRows() == 0 && !e.useOuterToBuild {
				e.finished.Store(true)
				return
			}
			jobFinished, buildErr := e.wait4BuildSide()
			if buildErr != nil {
				e.joinResultCh <- &hashjoinWorkerResult{
					err: buildErr,
				}
				return
			} else if jobFinished {
				return
			}
			hasWaitedForBuild = true
		}

		if probeSideResult.NumRows() == 0 {
			return
		}

		probeSideResource.dest <- probeSideResult
	}
}

func (e *MultiwayHashJoinExec) wait4BuildSide() (finished bool, err error) {
	done := make(chan error, 1)
	go func() {
		for _, fin := range e.buildFinished {
			tmp := <-fin
			if err != nil {
				done <-tmp
			}
		}
		close(done)
	}()
	select {
	case <-e.closeCh:
		return true, nil
	case err := <-done:
		if err != nil {
			return false, err
		}
	}
	hasEmpty := false
	for _, rc := range e.rowContainer {
		if rc.Len() == 0 {
			hasEmpty = true
			break
		}
	}
	if hasEmpty && (e.joinType == plannercore.InnerJoin || e.joinType == plannercore.SemiJoin) {
		return true, nil
	}
	return false, nil
}

//var buildSideResultLabel fmt.Stringer = stringutil.StringerStr("hashJoin.buildSideResult")

// fetchBuildSideRows fetches all rows from build side executor, and append them
// to e.buildSideResult.
func (e *MultiwayHashJoinExec) fetchBuildSideRows(ctx context.Context, b int, chkCh chan<- *chunk.Chunk, doneCh <-chan struct{}) {
	defer close(chkCh)
	var err error
	for {
		if e.finished.Load().(bool) {
			return
		}
		chk := chunk.NewChunkWithCapacity(e.buildSideExec[b].base().retFieldTypes, e.ctx.GetSessionVars().MaxChunkSize)
		err = Next(ctx, e.buildSideExec[b], chk)
		if err != nil {
			e.buildFinished[b] <- errors.Trace(err)
			return
		}
		failpoint.Inject("errorFetchBuildSideRowsMockOOMPanic", nil)
		if chk.NumRows() == 0 {
			return
		}
		select {
		case <-doneCh:
			return
		case <-e.closeCh:
			return
		case chkCh <- chk:
		}
	}
}

func (e *MultiwayHashJoinExec) initializeForProbe() {
	// e.probeResultChs is for transmitting the chunks which store the data of
	// probeSideExec, it'll be written by probe side worker goroutine, and read by join
	// workers.
	e.probeResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.probeResultChs[i] = make(chan *chunk.Chunk, 1)
	}

	// e.probeChkResourceCh is for transmitting the used probeSideExec chunks from
	// join workers to probeSideExec worker.
	e.probeChkResourceCh = make(chan *probeChkResource, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.probeChkResourceCh <- &probeChkResource{
			chk:  newFirstChunk(e.probeSideExec),
			dest: e.probeResultChs[i],
		}
	}

	// e.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to join worker goroutines.
	e.joinChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- newFirstChunk(e)
	}

	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.concurrency+1)
}

func (e *MultiwayHashJoinExec) fetchAndProbeHashTable(ctx context.Context) {
	e.initializeForProbe()
	e.joinWorkerWaitGroup.Add(1)
	go util.WithRecovery(func() { e.fetchProbeSideChunks(ctx) }, e.handleProbeSideFetcherPanic)

	probeKeyColIdx := make([]int, len(e.probeKeys))
	for i := range e.probeKeys {
		probeKeyColIdx[i] = e.probeKeys[i].Index
	}

	// Start e.concurrency join workers to probe hash table and join build side and
	// probe side rows.
	for i := uint(0); i < e.concurrency; i++ {
		e.joinWorkerWaitGroup.Add(1)
		workID := i
		go util.WithRecovery(func() { e.runJoinWorker(workID, probeKeyColIdx) }, e.handleJoinWorkerPanic)
	}
	go util.WithRecovery(e.waitJoinWorkersAndCloseResultChan, nil)
}

func (e *MultiwayHashJoinExec) handleProbeSideFetcherPanic(r interface{}) {
	for i := range e.probeResultChs {
		close(e.probeResultChs[i])
	}
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

func (e *MultiwayHashJoinExec) handleJoinWorkerPanic(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

//// Concurrently handling unmatched rows from the hash table
//func (e *MultiwayHashJoinExec) handleUnmatchedRowsFromHashTableInMemory(workerID uint) {
//	ok, joinResult := e.getNewJoinResult(workerID)
//	if !ok {
//		return
//	}
//	numChks := e.rowContainer.NumChunks()
//	for i := int(workerID); i < numChks; i += int(e.concurrency) {
//		chk := e.rowContainer.GetChunk(i)
//		for j := 0; j < chk.NumRows(); j++ {
//			if !e.outerMatchedStatus[i].UnsafeIsSet(j) { // process unmatched outer rows
//				e.joiners[workerID].onMissMatch(false, chk.GetRow(j), joinResult.chk)
//			}
//			if joinResult.chk.IsFull() {
//				e.joinResultCh <- joinResult
//				ok, joinResult = e.getNewJoinResult(workerID)
//				if !ok {
//					return
//				}
//			}
//		}
//	}
//
//	if joinResult == nil {
//		return
//	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
//		e.joinResultCh <- joinResult
//	}
//}
//
//// Sequentially handling unmatched rows from the hash table
//func (e *MultiwayHashJoinExec) handleUnmatchedRowsFromHashTableInDisk(workerID uint) {
//	ok, joinResult := e.getNewJoinResult(workerID)
//	if !ok {
//		return
//	}
//	numChks := e.rowContainer.NumChunks()
//	for i := 0; i < numChks; i++ {
//		numOfRows := e.rowContainer.NumRowsOfChunk(i)
//		for j := 0; j < numOfRows; j++ {
//			row, err := e.rowContainer.GetRow(chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)})
//			if err != nil {
//				// Catching the error and send it
//				joinResult.err = err
//				e.joinResultCh <- joinResult
//				return
//			}
//			if !e.outerMatchedStatus[i].UnsafeIsSet(j) { // process unmatched outer rows
//				e.joiners[workerID].onMissMatch(false, row, joinResult.chk)
//			}
//			if joinResult.chk.IsFull() {
//				e.joinResultCh <- joinResult
//				ok, joinResult = e.getNewJoinResult(workerID)
//				if !ok {
//					return
//				}
//			}
//		}
//	}
//	if joinResult == nil {
//		return
//	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
//		e.joinResultCh <- joinResult
//	}
//}

func (e *MultiwayHashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.joinWorkerWaitGroup.Wait()
	if e.useOuterToBuild {
		panic("experimental multiway join implementation does not support outer join")
		//if e.rowContainer.alreadySpilled() {
		//	// Sequentially handling unmatched rows from the hash table to avoid random accessing IO
		//	e.handleUnmatchedRowsFromHashTableInDisk(0)
		//} else {
		//	// Concurrently handling unmatched rows from the hash table at the tail
		//	for i := uint(0); i < e.concurrency; i++ {
		//		var workerID = i
		//		e.joinWorkerWaitGroup.Add(1)
		//		go util.WithRecovery(func() { e.handleUnmatchedRowsFromHashTableInMemory(workerID) }, e.handleJoinWorkerPanic)
		//	}
		//	e.joinWorkerWaitGroup.Wait()
		//}
	}
	close(e.joinResultCh)
}

func (e *MultiwayHashJoinExec) runJoinWorker(workerID uint, probeKeyColIdx []int) {
	var (
		probeSideResult *chunk.Chunk
		selected        = make([]bool, 0, chunk.InitialCapacity)
	)
	ok, joinResult := e.getNewJoinResult(workerID)
	if !ok {
		return
	}

	// Read and filter probeSideResult, and join the probeSideResult with the build side rows.
	emptyProbeSideResult := &probeChkResource{
		dest: e.probeResultChs[workerID],
	}
	hCtx := &hashContext{
		allTypes:  e.probeTypes,
		keyColIdx: probeKeyColIdx,
	}
	for ok := true; ok; {
		if e.finished.Load().(bool) {
			break
		}
		select {
		case <-e.closeCh:
			return
		case probeSideResult, ok = <-e.probeResultChs[workerID]:
		}
		if !ok {
			break
		}
		if e.useOuterToBuild {
			//ok, joinResult = e.join2ChunkForOuterHashJoin(workerID, probeSideResult, hCtx, joinResult)
		} else {
			ok, joinResult = e.join2Chunk(workerID, probeSideResult, hCtx, joinResult, selected)
		}
		if !ok {
			break
		}
		probeSideResult.Reset()
		emptyProbeSideResult.chk = probeSideResult
		e.probeChkResourceCh <- emptyProbeSideResult
	}
	// note joinResult.chk may be nil when getNewJoinResult fails in loops
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		e.joinResultCh <- joinResult
	} else if joinResult.chk != nil && joinResult.chk.NumRows() == 0 {
		e.joinChkResourceCh[workerID] <- joinResult.chk
	}
}

//func (e *MultiwayHashJoinExec) joinMatchedProbeSideRow2ChunkForOuterHashJoin(workerID uint, probeKey uint64, probeSideRow chunk.Row, hCtx *hashContext,
//	joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
//	buildSideRows, rowsPtrs, err := e.rowContainer.GetMatchedRowsAndPtrs(probeKey, probeSideRow, hCtx)
//	if err != nil {
//		joinResult.err = err
//		return false, joinResult
//	}
//	if len(buildSideRows) == 0 {
//		return true, joinResult
//	}
//
//	iter := chunk.NewIterator4Slice(buildSideRows)
//	var outerMatchStatus []outerRowStatusFlag
//	rowIdx := 0
//	for iter.Begin(); iter.Current() != iter.End(); {
//		outerMatchStatus, err = e.joiners[workerID].tryToMatchOuters(iter, probeSideRow, joinResult.chk, outerMatchStatus)
//		if err != nil {
//			joinResult.err = err
//			return false, joinResult
//		}
//		for i := range outerMatchStatus {
//			if outerMatchStatus[i] == outerRowMatched {
//				e.outerMatchedStatus[rowsPtrs[rowIdx+i].ChkIdx].Set(int(rowsPtrs[rowIdx+i].RowIdx))
//			}
//		}
//		rowIdx += len(outerMatchStatus)
//		if joinResult.chk.IsFull() {
//			e.joinResultCh <- joinResult
//			ok, joinResult := e.getNewJoinResult(workerID)
//			if !ok {
//				return false, joinResult
//			}
//		}
//	}
//	return true, joinResult
//}

func (e *MultiwayHashJoinExec) joinMatchedProbeSideRow2Chunk(workerID uint, probeKey uint64, probeSideRow chunk.Row, hCtx *hashContext,
	buildSideRows [][]chunk.Row, joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	numRows := make([]int, 0, e.numBuildExec + 1)
	for b := e.numBuildExec - 1; 0 <= b; b-- {
		numRows[b] = numRows[b + 1] * len(buildSideRows[b])
	}
	for i := 0; i < numRows[0]; i++ {
		for b := 0; b < e.numBuildExec; b++ {

		}
	}
	hasMatch, hasNull := false, false
	for iter.Begin(); iter.Current() != iter.End(); {
		matched, isNull, err := e.joiners[0][workerID].tryToMatchInners(probeSideRow, iter, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		hasMatch = hasMatch || matched
		hasNull = hasNull || isNull

		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult := e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	if !hasMatch {
		e.joiners[workerID].onMissMatch(hasNull, probeSideRow, joinResult.chk)
	}
	return true, joinResult
}

func (e *MultiwayHashJoinExec) getNewJoinResult(workerID uint) (bool, *hashjoinWorkerResult) {
	joinResult := &hashjoinWorkerResult{
		src: e.joinChkResourceCh[workerID],
	}
	ok := true
	select {
	case <-e.closeCh:
		ok = false
	case joinResult.chk, ok = <-e.joinChkResourceCh[workerID]:
	}
	return ok, joinResult
}

func (e *MultiwayHashJoinExec) join2Chunk(workerID uint, probeSideChk *chunk.Chunk, hCtx *hashContext,
	joinResult *hashjoinWorkerResult, selected []bool) (ok bool, _ *hashjoinWorkerResult) {
	var err error
	selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(probeSideChk), selected)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}

	hCtx.initHash(probeSideChk.NumRows())
	for _, i := range hCtx.keyColIdx {
		err = codec.HashChunkSelected(e.rowContainer[0].sc, hCtx.hashVals, probeSideChk, hCtx.allTypes[i], i, hCtx.buf, hCtx.hasNull, selected)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
	}

	matchedRows := make([][]chunk.Row, 0, e.numBuildExec)
	for i := range selected {
		if selected[i] {
			for b, rc := range e.rowContainer {
				matched, _, err := rc.GetMatchedRowsAndPtrs(hCtx.hashVals[i].Sum64(), probeSideChk.GetRow(i), hCtx)
				matchedRows[b] = matched
				if err != nil {
					joinResult.err = err;
					return false, joinResult
				}
				if len(matched) == 0 {
					selected[i] = false
					break
				}
			}
			if selected[i] {
				probeKey, probeRow := hCtx.hashVals[i].Sum64(), probeSideChk.GetRow(i)
				ok, joinResult = e.joinMatchedProbeSideRow2Chunk(workerID, probeKey, probeRow, hCtx, matchedRows, joinResult)
				if !ok {
					return false, joinResult
				}
			}
		}
	}
	return true, joinResult
}

//// join2ChunkForOuterHashJoin joins chunks when using the outer to build a hash table (refer to outer hash join)
//func (e *MultiwayHashJoinExec) join2ChunkForOuterHashJoin(workerID uint, probeSideChk *chunk.Chunk, hCtx *hashContext, joinResult *hashjoinWorkerResult) (ok bool, _ *hashjoinWorkerResult) {
//	hCtx.initHash(probeSideChk.NumRows())
//	for _, i := range hCtx.keyColIdx {
//		err := codec.HashChunkColumns(e.rowContainer[0].sc, hCtx.hashVals, probeSideChk, hCtx.allTypes[i], i, hCtx.buf, hCtx.hasNull)
//		if err != nil {
//			joinResult.err = err
//			return false, joinResult
//		}
//	}
//	for i := 0; i < probeSideChk.NumRows(); i++ {
//		probeKey, probeRow := hCtx.hashVals[i].Sum64(), probeSideChk.GetRow(i)
//		ok, joinResult = e.joinMatchedProbeSideRow2ChunkForOuterHashJoin(workerID, probeKey, probeRow, hCtx, joinResult)
//		if !ok {
//			return false, joinResult
//		}
//		if joinResult.chk.IsFull() {
//			e.joinResultCh <- joinResult
//			ok, joinResult = e.getNewJoinResult(workerID)
//			if !ok {
//				return false, joinResult
//			}
//		}
//	}
//	return true, joinResult
//}

// Next implements the Executor Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from build side child and build a hash table;
// step 2. fetch data from probe child in a background goroutine and probe the hash table in multiple join workers.
func (e *MultiwayHashJoinExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.prepared {
		e.buildFinished = make([]chan error, e.numBuildExec)
		for i := 0; i < e.numBuildExec; i++ {
			e.buildFinished[i] = make(chan error, 1)
			go util.WithRecovery(func() { e.fetchAndBuildHashTable(ctx, i) }, e.generateFetchAndBuildHashTablePanicHandler(i))
		}
		e.fetchAndProbeHashTable(ctx)
		e.prepared = true
	}
	if e.isOuterJoin {
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
	}
	req.Reset()

	result, ok := <-e.joinResultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		e.finished.Store(true)
		return result.err
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

func (e *MultiwayHashJoinExec) generateFetchAndBuildHashTablePanicHandler(b int) func(interface{}) {
	return func(r interface{}) {
		if r != nil {
			e.buildFinished[b] <- errors.Errorf("%v", r)
		}
		close(e.buildFinished[b])
	}
}

func (e *MultiwayHashJoinExec) fetchAndBuildHashTable(ctx context.Context, b int) {
	// buildSideResultCh transfers build side chunk from build side fetch to build hash table.
	buildSideResultCh := make(chan *chunk.Chunk, 1)
	doneCh := make(chan struct{})
	fetchBuildSideRowsOk := make(chan error, 1)
	go util.WithRecovery(
		func() { e.fetchBuildSideRows(ctx, b, buildSideResultCh, doneCh) },
		func(r interface{}) {
			if r != nil {
				fetchBuildSideRowsOk <- errors.Errorf("%v", r)
			}
			close(fetchBuildSideRowsOk)
		},
	)

	// TODO: Parallel build hash table. Currently not support because `rowHashMap` is not thread-safe.
	err := e.buildHashTableForList(b, buildSideResultCh)
	if err != nil {
		e.buildFinished[b] <- errors.Trace(err)
		close(doneCh)
	}
	// Wait fetchBuildSideRows be finished.
	// 1. if buildHashTableForList fails
	// 2. if probeSideResult.NumRows() == 0, fetchProbeSideChunks will not wait for the build side.
	for range buildSideResultCh {
	}
	// Check whether err is nil to avoid sending redundant error into buildFinished.
	if err == nil {
		if err = <-fetchBuildSideRowsOk; err != nil {
			e.buildFinished[b] <- err
		}
	}
}

// buildHashTableForList builds hash table from `list`.
func (e *MultiwayHashJoinExec) buildHashTableForList(b int, buildSideResultCh <-chan *chunk.Chunk) error {
	buildKeyColIdx := make([]int, len(e.buildKeys))
	for i := range e.buildKeys {
		buildKeyColIdx[i] = e.buildKeys[b][i].Index
	}
	hCtx := &hashContext{
		allTypes:  e.buildTypes[b],
		keyColIdx: buildKeyColIdx,
	}
	var err error
	var selected []bool
	e.rowContainer[b] = newHashRowContainer(e.ctx, int(e.buildSideEstCount[b]), hCtx)
	e.rowContainer[b].GetMemTracker().AttachTo(e.memTracker)
	e.rowContainer[b].GetMemTracker().SetLabel(buildSideResultLabel)
	e.rowContainer[b].GetDiskTracker().AttachTo(e.diskTracker)
	e.rowContainer[b].GetDiskTracker().SetLabel(buildSideResultLabel)
	if config.GetGlobalConfig().OOMUseTmpStorage {
		actionSpill := e.rowContainer[b].ActionSpill()
		e.ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(actionSpill)
	}
	for chk := range buildSideResultCh {
		if e.finished.Load().(bool) {
			return nil
		}
		if !e.useOuterToBuild {
			err = e.rowContainer[b].PutChunk(chk)
		} else {
			var bitMap = bitmap.NewConcurrentBitmap(chk.NumRows())
			e.outerMatchedStatus = append(e.outerMatchedStatus, bitMap)
			e.memTracker.Consume(bitMap.BytesConsumed())
			if len(e.outerFilter) == 0 {
				err = e.rowContainer[b].PutChunk(chk)
			} else {
				selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(chk), selected)
				if err != nil {
					return err
				}
				err = e.rowContainer[b].PutChunkSelected(chk, selected)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

