// Copyright 2018 PingCAP, Inc.
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

package chunk

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"

	"go.uber.org/zap"
)

// RowContainer provides a place for many rows, so many that we might want to spill them into disk
type RowContainer struct {
	// records stores the chunks in memory.
	records *List
	// recordsInDisk stores the chunks in disk.
	recordsInDisk *ListInDisk
	// tmp is the last chunk in RowContainer which is not managed by the RowContainer
	tmp *Chunk

	fieldType []*types.FieldType
	chunkSize int
	numRow    int

	// exceeded indicates that records have exceeded memQuota during
	// this PutChunk and we should spill now.
	// It's for concurrency usage, so access it with atomic.
	exceeded uint32
	// spilled indicates that records have spilled out into disk.
	// It's for concurrency usage, so access it with atomic.
	spilled uint32

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker
	actionSpill *spillDiskAction
}

// NewRowContainer creates a new RowContainer in memory
func NewRowContainer(fieldType []*types.FieldType, chunkSize int) *RowContainer {
	li := NewList(fieldType, chunkSize, chunkSize)
	rc := &RowContainer{records: li, fieldType: fieldType, chunkSize: chunkSize}
	rc.memTracker = li.memTracker
	rc.diskTracker = disk.NewTracker(stringutil.StringerStr("RowContainer"), -1)
	return rc
}

func (c *RowContainer) spillToDisk() (err error) {
	N := c.records.NumChunks()
	c.recordsInDisk = NewListInDisk(c.records.FieldTypes())
	c.recordsInDisk.diskTracker.AttachTo(c.diskTracker)
	for i := 0; i < N; i++ {
		chk := c.records.GetChunk(i)
		err = c.recordsInDisk.Add(chk)
		if err != nil {
			return
		}
	}
	c.records.Clear()
	return
}

// Reset resets RowContainer
func (c *RowContainer) Reset() error {
	if c.AlreadySpilled() {
		err := c.recordsInDisk.Close()
		c.recordsInDisk = nil
		if err != nil {
			return err
		}
		atomic.StoreUint32(&c.exceeded, 0)
		atomic.StoreUint32(&c.spilled, 0)
		c.actionSpill.Reset()
	} else {
		c.records.Reset()
	}
	c.tmp = nil
	return nil
}

// AlreadySpilled indicates that records have spilled out into disk.
func (c *RowContainer) AlreadySpilled() bool { return c.recordsInDisk != nil }

// AlreadySpilledSafe indicates that records have spilled out into disk. It's thread-safe.
func (c *RowContainer) AlreadySpilledSafe() bool { return atomic.LoadUint32(&c.spilled) == 1 }

// NumRow returns the number of rows in the container
func (c *RowContainer) NumRow() int {
	num := c.numRowsInTemporary()
	if c.AlreadySpilled() {
		return c.recordsInDisk.Len() + num
	}
	return c.records.Len() + num
}

// NumRowsOfChunk returns the number of rows of a chunk in the ListInDisk.
func (c *RowContainer) NumRowsOfChunk(chkID int) int {
	if c.tmp != nil && chkID == c.NumChunks() - 1 {
		return c.tmp.NumRows()
	}
	if c.AlreadySpilled() {
		return c.recordsInDisk.NumRowsOfChunk(chkID)
	}
	return c.records.NumRowsOfChunk(chkID)
}

// NumChunks returns the number of chunks in the container.
func (c *RowContainer) NumChunks() int {
	var numTmp int
	if c.tmp == nil {
		numTmp = 0
	} else {
		numTmp = 1
	}
	if c.AlreadySpilled() {
		return c.recordsInDisk.NumChunks() + numTmp
	}
	return c.records.NumChunks() + numTmp
}

// Add appends a chunk into the RowContainer
func (c *RowContainer) Add(chk *Chunk) (err error) {
	if c.AlreadySpilled() {
		err = c.recordsInDisk.Add(chk)
	} else {
		c.records.Add(chk)
		if atomic.LoadUint32(&c.exceeded) != 0 {
			err = c.spillToDisk()
			if err != nil {
				return err
			}
			atomic.StoreUint32(&c.spilled, 1)
		}
	}
	return
}

func (c *RowContainer) numRowsInTemporary() int {
	if c.tmp == nil {
		return 0
	}
	return c.tmp.NumRows()
}

// SetTemporary appends an additional chunk at the end of the RowContainer, it is not managed by the RowContainer.
func (c *RowContainer) SetTemporary(chk *Chunk) {
	c.tmp = chk
}

// AllocChunk allocates a new chunk from RowContainer
func (c *RowContainer) AllocChunk() (chk *Chunk) {
	return c.records.allocChunk()
}

// AppendRow copies one row into RowContainer, it is not thread safe
func (c *RowContainer) AppendRow(row Row) (ptr RowPtr, err error) {
	if c.AlreadySpilled() {
		ptr, err = c.recordsInDisk.AppendRow(row)
	} else {
		ptr = c.records.AppendRow(row)
		if atomic.LoadUint32(&c.exceeded) != 0 {
			err = c.spillToDisk()
			if err != nil {
				return
			}
			atomic.StoreUint32(&c.spilled, 1)
		}
	}
	return
}

// GetChunk returns chkIdx th chunk of in memory records
func (c *RowContainer) GetChunk(chkIdx int) *Chunk {
	return c.records.GetChunk(chkIdx)
}

// GetRow returns the row the ptr pointed to
func (c *RowContainer) GetRow(ptr RowPtr) (Row, error) {
	if c.tmp != nil && ptr.ChkIdx == uint32(c.NumChunks() - 1) {
		return c.tmp.GetRow(int(ptr.RowIdx)), nil
	}
	if c.AlreadySpilled() {
		return c.recordsInDisk.GetRow(ptr)
	}
	return c.records.GetRow(ptr), nil
}

// GetMemTracker returns the memory tracker in records, panics if the RowContainer has already spilled
func (c *RowContainer) GetMemTracker() *memory.Tracker {
	return c.memTracker
}

// GetDiskTracker returns the underlying disk usage tracker in recordsInDisk.
func (c *RowContainer) GetDiskTracker() *disk.Tracker {
	return c.diskTracker
}

// Close close the RowContainer
func (c *RowContainer) Close() (err error) {
	if c.AlreadySpilled() {
		err = c.recordsInDisk.Close()
		c.recordsInDisk = nil
	}
	return
}

// ActionSpill returns a memory.ActionOnExceed for spilling over to disk.
func (c *RowContainer) ActionSpill() memory.ActionOnExceed {
	c.actionSpill = &spillDiskAction{c: c}
	return c.actionSpill
}

// spillDiskAction implements memory.ActionOnExceed for chunk.List. If
// the memory quota of a query is exceeded, spillDiskAction.Action is
// triggered.
type spillDiskAction struct {
	once           sync.Once
	c              *RowContainer
	fallbackAction memory.ActionOnExceed
}

// Action sends a signal to trigger spillToDisk method of RowContainer
// and if it is already triggered before, call its fallbackAction.
func (a *spillDiskAction) Action(t *memory.Tracker) {
	if a.c.AlreadySpilledSafe() {
		if a.fallbackAction != nil {
			a.fallbackAction.Action(t)
		}
	}
	a.once.Do(func() {
		atomic.StoreUint32(&a.c.exceeded, 1)
		logutil.BgLogger().Info("memory exceeds quota, spill to disk now.", zap.String("memory", t.String()))
	})
}

func (a *spillDiskAction) SetFallback(fallback memory.ActionOnExceed) {
	a.fallbackAction = fallback
}

func (a *spillDiskAction) SetLogHook(hook func(uint64)) {}

func (a *spillDiskAction) Reset() {
	a.once = sync.Once{}
}
