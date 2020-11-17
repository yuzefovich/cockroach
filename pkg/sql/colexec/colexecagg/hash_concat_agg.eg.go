// size of LocalOnlySessionData is 216
// size of CmpOp is 72
// size of SelectClause is 168
// Code generated by execgen; DO NOT EDIT.
// Copyright 2020 The Cockroach Authors.
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
)

func newConcatHashAggAlloc(allocator *colmem.Allocator, allocSize int64) aggregateFuncAlloc {
	return &concatHashAggAlloc{aggAllocBase: aggAllocBase{
		allocator: allocator,
		allocSize: allocSize,
	}}
}

type concatHashAgg struct {
	hashAggregateFuncBase
	allocator *colmem.Allocator
	// curAgg holds the running total.
	curAgg []byte
	// col points to the output vector we are updating.
	col *coldata.Bytes
	// vec is the same as col before conversion from coldata.Vec.
	vec coldata.Vec
	// foundNonNullForCurrentGroup tracks if we have seen any non-null values
	// for the group that is currently being aggregated.
	foundNonNullForCurrentGroup bool
}

func (a *concatHashAgg) Init(groups []bool, vec coldata.Vec) {
	a.hashAggregateFuncBase.Init(groups, vec)
	a.vec = vec
	a.col = vec.Bytes()
	a.Reset()
}

func (a *concatHashAgg) Reset() {
	a.hashAggregateFuncBase.Reset()
	a.foundNonNullForCurrentGroup = false
	a.curAgg = zeroBytesValue
}

func (a *concatHashAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
	oldCurAggSize := len(a.curAgg)
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.Bytes(), vec.Nulls()
	a.allocator.PerformOperation(
		[]coldata.Vec{a.vec},
		func() {
			{
				sel = sel[:inputLen]
				if nulls.MaybeHasNulls() {
					for _, i := range sel {

						var isNull bool
						isNull = nulls.NullAt(i)
						if !isNull {
							a.curAgg = append(a.curAgg, col.Get(i)...)
							a.foundNonNullForCurrentGroup = true
						}
					}
				} else {
					for _, i := range sel {

						var isNull bool
						isNull = false
						if !isNull {
							a.curAgg = append(a.curAgg, col.Get(i)...)
							a.foundNonNullForCurrentGroup = true
						}
					}
				}
			}
		},
	)
	newCurAggSize := len(a.curAgg)
	a.allocator.AdjustMemoryUsage(int64(newCurAggSize - oldCurAggSize))
}

func (a *concatHashAgg) Flush(outputIdx int) {
	if !a.foundNonNullForCurrentGroup {
		a.nulls.SetNull(outputIdx)
	} else {
		a.col.Set(outputIdx, a.curAgg)
	}
	// Release the reference to curAgg eagerly.
	a.allocator.AdjustMemoryUsage(-int64(len(a.curAgg)))
	a.curAgg = nil
}

type concatHashAggAlloc struct {
	aggAllocBase
	aggFuncs []concatHashAgg
}

var _ aggregateFuncAlloc = &concatHashAggAlloc{}

const sizeOfConcatHashAgg = int64(unsafe.Sizeof(concatHashAgg{}))
const concatHashAggSliceOverhead = int64(unsafe.Sizeof([]concatHashAgg{}))

func (a *concatHashAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(concatHashAggSliceOverhead + sizeOfConcatHashAgg*a.allocSize)
		a.aggFuncs = make([]concatHashAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}
