// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// IsHashGroupJoinerSupported determines whether we can plan a vectorized hash
// group-join operator instead of a hash join followed by a hash aggregator
// (with possible projection in-between).
//
// numLeftInputCols and numRightInputCols specify the width of inputs to the
// hash joiner.
func IsHashGroupJoinerSupported(
	numLeftInputCols, numRightInputCols uint32,
	hj *execinfrapb.HashJoinerSpec,
	joinPostProcessSpec *execinfrapb.PostProcessSpec,
	agg *execinfrapb.AggregatorSpec,
) bool {
	// Check that the hash join core is supported.
	if hj.Type != descpb.InnerJoin {
		// TODO(yuzefovich): relax this for left/right/full outer joins.
		return false
	}
	if !hj.OnExpr.Empty() {
		return false
	}
	// Check that the post-process spec only has a projection.
	if joinPostProcessSpec.RenderExprs != nil ||
		joinPostProcessSpec.Limit != 0 ||
		joinPostProcessSpec.Offset != 0 {
		return false
	}

	joinOutputProjection := joinPostProcessSpec.OutputColumns
	if !joinPostProcessSpec.Projection {
		joinOutputProjection = make([]uint32, numLeftInputCols+numRightInputCols)
		for i := range joinOutputProjection {
			joinOutputProjection[i] = uint32(i)
		}
	}

	// Check that the aggregator's grouping columns are the same as the join's
	// equality columns.
	if len(agg.GroupCols) != len(hj.LeftEqColumns) {
		return false
	}
	for _, groupCol := range agg.GroupCols {
		// Grouping columns refer to the output columns of the join, so we need
		// to remap it using the projection slice.
		joinOutCol := joinOutputProjection[groupCol]
		found := false
		eqCols := hj.LeftEqColumns
		if joinOutCol >= numLeftInputCols {
			// This grouping column comes from the right side, so we'll look at
			// the right equality columns.
			joinOutCol -= numLeftInputCols
			eqCols = hj.RightEqColumns
		}
		for _, eqCol := range eqCols {
			if joinOutCol == eqCol {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	for i := range agg.Aggregations {
		aggFn := &agg.Aggregations[i]
		if !colexecagg.IsAggOptimized(aggFn.Func) {
			// We currently only support the optimized aggregate functions.
			// TODO(yuzefovich): add support for this.
			return false
		}
		if aggFn.Distinct || aggFn.FilterColIdx != nil {
			// We currently don't support distinct and filtering aggregations.
			// TODO(yuzefovich): add support for this.
			return false
		}
	}

	return true
}

type hashGroupJoinerState int

const (
	hgjBuilding hashGroupJoinerState = iota
	hgjProbing
	hgjProcessingUnmatched
	hgjOutputting
	hgjDone
)

func TryHashGroupJoiner(args *colexecagg.NewAggregatorArgs) (colexecbase.Operator, error) {
	var hgj colexecbase.Operator
	if err := colexecerror.CatchVectorizedRuntimeError(func() {
		input := args.Input
		sp, isSimpleProjection := input.(*simpleProjectOp)
		var joinOutputProjection []uint32
		if isSimpleProjection {
			joinOutputProjection = sp.projection
			input = sp.input
		}
		dp, isDiskSpiller := input.(*diskSpillerBase)
		if !isDiskSpiller {
			colexecerror.InternalError(errors.New("not a disk spiller"))
		}
		hj, isHashJoiner := dp.inMemoryOp.(*hashJoiner)
		if !isHashJoiner {
			colexecerror.InternalError(errors.New("not a hash joiner"))
		}
		args.Allocator = hj.outputUnlimitedAllocator
		hgj = newHashGroupJoiner(
			hj.outputUnlimitedAllocator,
			hj.spec,
			hj.inputOne,
			hj.inputTwo,
			joinOutputProjection,
			args,
		)
	}); err != nil {
		return nil, err
	}
	return hgj, nil
}

// joinOutputProjection specifies a simple projection on top of the hash join
// output, it can be nil if all columns from the hash join are output.
func newHashGroupJoiner(
	unlimitedAllocator *colmem.Allocator,
	spec HashJoinerSpec,
	leftSource, rightSource colexecbase.Operator,
	joinOutputProjection []uint32,
	args *colexecagg.NewAggregatorArgs,
) colexecbase.Operator {
	switch spec.joinType {
	case descpb.InnerJoin, descpb.RightOuterJoin, descpb.FullOuterJoin:
	default:
		colexecerror.InternalError(errors.AssertionFailedf("hash group join is only supported for inner, right/full outer joins"))
	}
	var da rowenc.DatumAlloc
	helper := newAggregatorHelper(args, &da, true /* isHashAgg */, coldata.BatchSize())
	if _, ok := helper.(*defaultAggregatorHelper); !ok {
		colexecerror.InternalError(errors.AssertionFailedf("only non-distinct and non-filtering aggregations are supported"))
	}
	for _, aggFn := range args.Spec.Aggregations {
		if !colexecagg.IsAggOptimized(aggFn.Func) {
			colexecerror.InternalError(errors.AssertionFailedf("only optimized aggregate functions are supported"))
		}
	}
	aggFnsAlloc, _, toClose, err := colexecagg.NewAggregateFuncsAlloc(
		args, hashAggregatorAllocSize, true, /* isHashAgg */
	)
	if err != nil {
		colexecerror.InternalError(err)
	}
	if joinOutputProjection == nil {
		joinOutputProjection = make([]uint32, len(spec.left.sourceTypes)+len(spec.right.sourceTypes))
		for i := range joinOutputProjection {
			joinOutputProjection[i] = uint32(i)
		}
	}
	aggFnUsesLeft := make([]bool, len(args.Spec.Aggregations))
	for i := range args.Spec.Aggregations {
		var usesLeftColumn, usesRightColumn bool
		for _, colIdx := range args.Spec.Aggregations[i].ColIdx {
			joinOutColIdx := joinOutputProjection[colIdx]
			if int(joinOutColIdx) < len(spec.left.sourceTypes) {
				usesLeftColumn = true
			} else {
				usesRightColumn = true
			}
		}
		if usesLeftColumn && usesRightColumn {
			colexecerror.InternalError(errors.AssertionFailedf(
				"hash group join with an aggregate function that uses columns from both sides of the join is not supported",
			))
		}
		aggFnUsesLeft[i] = usesLeftColumn
	}
	return &hashGroupJoiner{
		hj: NewHashJoiner(
			unlimitedAllocator,
			unlimitedAllocator,
			spec,
			leftSource,
			rightSource,
			HashJoinerInitialNumBuckets,
		).(*hashJoiner),
		allocator:            unlimitedAllocator,
		joinOutputProjection: joinOutputProjection,
		aggSpec:              args.Spec,
		aggFnUsesLeft:        aggFnUsesLeft,
		aggFnsAlloc:          aggFnsAlloc,
		rightSideFakeSel:     make([]int, coldata.MaxBatchSize),
		outputTypes:          args.OutputTypes,
		toClose:              toClose,
	}
}

type hashGroupJoiner struct {
	hj *hashJoiner

	state     hashGroupJoinerState
	allocator *colmem.Allocator

	joinOutputProjection []uint32
	aggSpec              *execinfrapb.AggregatorSpec
	aggFnUsesLeft        []bool
	aggInputVecsScratch  []coldata.Vec

	// rightTupleIdxToBucketIdx maps from the position of the tuple in the right
	// input to the bucket that it belongs to.
	rightTupleIdxToBucketIdx []int
	// bucketIdxToBucketStart maps from the bucket index to the first tuple in
	// rightTuplesArrangedByBuckets that belongs to the bucket.
	bucketIdxToBucketStart []int
	// rightTuplesArrangedByBuckets are concatenated "selection vectors" that
	// arrange the indices of right tuples in such a manner that all tuples
	// from the same bucket are contiguous. It holds that within a bucket the
	// indices are increasing and that all tuples from the same bucket are
	// determined by
	// rightTuplesArrangedByBuckets[
	//   bucketIdxToBucketStart[bucket] : bucketIdxToBucketStart[bucket+1]
	// ].
	rightTuplesArrangedByBuckets []int
	buckets                      []aggBucket
	bucketMatched                []bool
	aggFnsAlloc                  *colexecagg.AggregateFuncsAlloc
	rightSideFakeSel             []int

	output      coldata.Batch
	outputTypes []*types.T
	bucketIdx   int

	toClose colexecbase.Closers
}

var _ closableOperator = &hashGroupJoiner{}

func (h *hashGroupJoiner) Init() {
	h.hj.Init()
	// TODO(yuzefovich): this is a bit hacky.
	h.hj.ht.allowNullEquality = true
}

func (h *hashGroupJoiner) Next(ctx context.Context) coldata.Batch {
	isInnerJoin := h.hj.spec.joinType == descpb.InnerJoin
	for {
		switch h.state {
		case hgjBuilding:
			h.hj.build(ctx, true)
			// TODO(yuzefovich): check whether the hash table is empty and
			// optimize the behavior if so.
			if h.hj.ht.vals.Length() == 0 {
				colexecerror.InternalError(errors.AssertionFailedf("empty right input is not supported"))
			}

			h.aggInputVecsScratch = make([]coldata.Vec, len(h.joinOutputProjection))
			for i, joinOutColIdx := range h.joinOutputProjection {
				if int(joinOutColIdx) >= len(h.hj.spec.left.sourceTypes) {
					h.aggInputVecsScratch[i] = h.hj.ht.vals.colVecs[int(joinOutColIdx)-len(h.hj.spec.left.sourceTypes)]
				}
			}

			// Divide up all tuples from the right input into buckets. All
			// tuples non-distinct on the equality columns will have the same
			// headID value which equals the position of the earliest tuple from
			// this set in h.hj.ht.vals. By construction, whenever a larger headID
			// is seen, it is the first tuple of the new bucket.
			h.hj.ht.findBuckets(
				h.hj.ht.vals, h.hj.ht.keys, h.hj.ht.buildScratch.first,
				h.hj.ht.buildScratch.next, h.hj.ht.checkProbeForDistinct,
			)
			// TODO: memory accounting.
			h.rightTupleIdxToBucketIdx = make([]int, h.hj.ht.vals.Length())
			numBuckets := 1
			largestHeadID := uint64(1)
			h.rightTupleIdxToBucketIdx[0] = 0
			for i := 1; i < h.hj.ht.vals.Length(); i++ {
				curHeadID := h.hj.ht.probeScratch.headID[i]
				if curHeadID > largestHeadID {
					largestHeadID = curHeadID
					h.rightTupleIdxToBucketIdx[i] = numBuckets
					numBuckets++
				} else {
					h.rightTupleIdxToBucketIdx[i] = h.rightTupleIdxToBucketIdx[curHeadID-1]
				}
			}

			// Populate other internal slices for efficient lookups of all right
			// tuples from a bucket.
			// TODO: we can skip this if right equality columns form a key.
			h.rightTuplesArrangedByBuckets = make([]int, h.hj.ht.vals.Length())
			for i := range h.rightTuplesArrangedByBuckets {
				h.rightTuplesArrangedByBuckets[i] = i
			}
			sort.SliceStable(h.rightTuplesArrangedByBuckets, func(i, j int) bool {
				return h.rightTupleIdxToBucketIdx[h.rightTuplesArrangedByBuckets[i]] < h.rightTupleIdxToBucketIdx[h.rightTuplesArrangedByBuckets[j]]
			})
			h.bucketIdxToBucketStart = make([]int, numBuckets+1)
			bucketIdx := 0
			prevRightTupleIdx := 0
			for i, rightTupleIdx := range h.rightTuplesArrangedByBuckets[1:] {
				if h.rightTupleIdxToBucketIdx[prevRightTupleIdx] != h.rightTupleIdxToBucketIdx[rightTupleIdx] {
					bucketIdx++
					h.bucketIdxToBucketStart[bucketIdx] = i + 1
				}
				prevRightTupleIdx = rightTupleIdx
			}
			h.bucketIdxToBucketStart[numBuckets] = h.hj.ht.vals.Length()

			h.buckets = make([]aggBucket, numBuckets)
			h.bucketMatched = make([]bool, numBuckets)
			// TODO: this is very hacky.
			h.hj.spec.rightDistinct = true
			h.state = hgjProbing
			continue

		case hgjProbing:
			batch := h.hj.inputOne.Next(ctx)
			if batch.Length() == 0 {
				if isInnerJoin {
					h.state = hgjOutputting
				} else {
					h.state = hgjProcessingUnmatched
				}
				continue
			}
			for i, joinOutColIdx := range h.joinOutputProjection {
				if int(joinOutColIdx) < len(h.hj.spec.left.sourceTypes) {
					h.aggInputVecsScratch[i] = batch.ColVec(int(joinOutColIdx))
				}
			}

			if nResults := h.hj.initialProbeAndCollect(ctx, batch); nResults > 0 {
				// TODO(yuzefovich): pay attention to
				// h.hj.probeState.probeRowUnmatched for full outer joins.
				for i := 0; i < nResults; {
					firstMatchIdx := i
					rightSideTupleIdx := h.hj.probeState.buildIdx[i]
					i++
					// All matches with the same tuple on the right side are
					// contiguous.
					for ; i < nResults && h.hj.probeState.buildIdx[i] == rightSideTupleIdx; i++ {
					}
					firstNonMatchIdx := i
					bucketIdx := h.rightTupleIdxToBucketIdx[rightSideTupleIdx]
					bucket := &h.buckets[bucketIdx]
					if !h.bucketMatched[bucketIdx] {
						h.bucketMatched[bucketIdx] = true
						bucket.fns = h.aggFnsAlloc.MakeAggregateFuncs()
						for _, fn := range bucket.fns {
							fn.Init(nil /* groups */)
						}
					}
					numMatched := firstNonMatchIdx - firstMatchIdx
					bucketStart := h.bucketIdxToBucketStart[bucketIdx]
					bucketEnd := h.bucketIdxToBucketStart[bucketIdx+1]
					for _, rightSideTupleIdx := range h.rightTuplesArrangedByBuckets[bucketStart:bucketEnd] {
						for j := 0; j < numMatched; j++ {
							h.rightSideFakeSel[j] = rightSideTupleIdx
						}
						for fnIdx, fn := range bucket.fns {
							sel := h.rightSideFakeSel
							if h.aggFnUsesLeft[fnIdx] {
								sel = h.hj.probeState.probeIdx[firstMatchIdx:firstNonMatchIdx]
							}
							fn.Compute(
								h.aggInputVecsScratch, h.aggSpec.Aggregations[fnIdx].ColIdx, numMatched, sel,
							)
						}
					}
				}
			}

		case hgjProcessingUnmatched:
			for bucketIdx := range h.buckets {
				if !h.bucketMatched[bucketIdx] {
					bucket := &h.buckets[bucketIdx]
					bucket.fns = h.aggFnsAlloc.MakeAggregateFuncs()
					for _, fn := range bucket.fns {
						fn.Init(nil /* groups */)
					}
					bucketStart := h.bucketIdxToBucketStart[bucketIdx]
					bucketEnd := h.bucketIdxToBucketStart[bucketIdx+1]
					numTuplesProcessed := 0
					sel := h.rightSideFakeSel
					for numTuplesProcessed < bucketEnd-bucketStart {
						n := copy(sel, h.rightTuplesArrangedByBuckets[bucketStart+numTuplesProcessed:bucketEnd])
						for fnIdx, fn := range bucket.fns {
							// We only have anything to compute if the aggregate
							// function takes in an argument from the right
							// side.
							if !h.aggFnUsesLeft[fnIdx] {
								fn.Compute(
									h.aggInputVecsScratch, h.aggSpec.Aggregations[fnIdx].ColIdx, n, sel,
								)
							}
						}
						numTuplesProcessed += n
					}
				}
			}
			h.state = hgjOutputting

		case hgjOutputting:
			h.output, _ = h.allocator.ResetMaybeReallocate(h.outputTypes, h.output, len(h.buckets))
			curOutputIdx := 0
			h.allocator.PerformOperation(h.output.ColVecs(), func() {
				for curOutputIdx < h.output.Capacity() && h.bucketIdx < len(h.buckets) {
					if !isInnerJoin || h.bucketMatched[h.bucketIdx] {
						bucket := h.buckets[h.bucketIdx]
						for fnIdx, fn := range bucket.fns {
							fn.SetOutput(h.output.ColVec(fnIdx))
							fn.Flush(curOutputIdx)
						}
						curOutputIdx++
					}
					h.bucketIdx++
				}
			})
			if h.bucketIdx == len(h.buckets) {
				h.state = hgjDone
			}
			h.output.SetLength(curOutputIdx)
			return h.output

		case hgjDone:
			return coldata.ZeroBatch

		default:
			colexecerror.InternalError(errors.AssertionFailedf("unknown hashGroupJoinerState %d", h.state))
			// Unreachable code.
			return nil
		}
	}
}

func (h *hashGroupJoiner) ChildCount(verbose bool) int {
	return h.hj.ChildCount(verbose)
}

func (h *hashGroupJoiner) Child(nth int, verbose bool) execinfra.OpNode {
	return h.hj.Child(nth, verbose)
}

func (h *hashGroupJoiner) Close(ctx context.Context) error {
	return h.toClose.Close(ctx)
}
