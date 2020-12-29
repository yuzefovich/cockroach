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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecagg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestHashGroupJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(ctx)

	for _, tc := range []struct {
		description       string
		jtc               joinTestCase
		joinOutProjection []uint32
		atc               aggregatorTestCase
	}{
		{
			description: "inner join without projection",
			jtc: joinTestCase{
				joinType:     descpb.InnerJoin,
				leftTuples:   tuples{{1, 1}, {2, 3}, {4, 4}, {1, 2}},
				rightTuples:  tuples{{1, -1}, {3, -6}, {2, -5}, {2, nil}, {1, -2}, {2, -3}},
				leftTypes:    rowenc.TwoIntCols,
				rightTypes:   rowenc.TwoIntCols,
				leftOutCols:  []uint32{0, 1},
				rightOutCols: []uint32{0, 1},
				leftEqCols:   []uint32{0},
				rightEqCols:  []uint32{0},
			},
			atc: aggregatorTestCase{
				typs: []*types.T{types.Int, types.Int, types.Int, types.Int},
				aggFns: []execinfrapb.AggregatorSpec_Func{
					execinfrapb.AggregatorSpec_ANY_NOT_NULL,
					execinfrapb.AggregatorSpec_SUM_INT,
					execinfrapb.AggregatorSpec_ANY_NOT_NULL,
					execinfrapb.AggregatorSpec_MIN,
				},
				groupCols: []uint32{0},
				aggCols:   [][]uint32{{0}, {1}, {2}, {3}},
				expected:  tuples{{1, 6, 1, -2}, {2, 9, 2, -5}},
			},
		},
		{
			description: "inner join with projection",
			jtc: joinTestCase{
				joinType:     descpb.InnerJoin,
				leftTuples:   tuples{{2, 3}, {1, 1}, {4, 4}, {1, 2}},
				rightTuples:  tuples{{1, -2}, {2, -3}, {1, -1}, {3, -6}, {2, -5}, {2, nil}},
				leftTypes:    rowenc.TwoIntCols,
				rightTypes:   rowenc.TwoIntCols,
				leftOutCols:  []uint32{0, 1},
				rightOutCols: []uint32{1},
				leftEqCols:   []uint32{0},
				rightEqCols:  []uint32{0},
			},
			joinOutProjection: []uint32{0, 1, 3},
			atc: aggregatorTestCase{
				typs: []*types.T{types.Int, types.Int, types.Int},
				aggFns: []execinfrapb.AggregatorSpec_Func{
					execinfrapb.AggregatorSpec_ANY_NOT_NULL,
					execinfrapb.AggregatorSpec_MAX,
					execinfrapb.AggregatorSpec_COUNT,
				},
				groupCols: []uint32{0},
				aggCols:   [][]uint32{{0}, {1}, {2}},
				expected:  tuples{{1, 2, 4}, {2, 3, 2}},
			},
		},
		{
			description: "right outer join",
			jtc: joinTestCase{
				joinType:     descpb.RightOuterJoin,
				leftTuples:   tuples{{2, 3}, {1, 1}, {4, 4}, {1, 2}},
				rightTuples:  tuples{{1, -2}, {3, -7}, {2, -3}, {1, -1}, {3, -6}, {0, nil}, {2, -5}, {2, nil}},
				leftTypes:    rowenc.TwoIntCols,
				rightTypes:   rowenc.TwoIntCols,
				leftOutCols:  []uint32{1},
				rightOutCols: []uint32{0, 1},
				leftEqCols:   []uint32{0},
				rightEqCols:  []uint32{0},
			},
			joinOutProjection: []uint32{2, 1, 3},
			atc: aggregatorTestCase{
				typs: []*types.T{types.Int, types.Int, types.Int},
				aggFns: []execinfrapb.AggregatorSpec_Func{
					execinfrapb.AggregatorSpec_ANY_NOT_NULL,
					execinfrapb.AggregatorSpec_MAX,
					execinfrapb.AggregatorSpec_COUNT,
				},
				groupCols: []uint32{0},
				aggCols:   [][]uint32{{0}, {1}, {2}},
				expected:  tuples{{0, nil, 0}, {1, 2, 4}, {2, 3, 2}, {3, nil, 2}},
			},
		},
	} {
		log.Infof(ctx, "%s", tc.description)
		tc.jtc.init()
		require.NoError(t, tc.atc.init())
		constructors, constArguments, outputTypes, err := colexecagg.ProcessAggregations(
			&evalCtx, nil /* semaCtx */, tc.atc.spec.Aggregations, tc.atc.typs,
		)
		require.NoError(t, err)
		runTests(
			t, []tuples{tc.jtc.leftTuples, tc.jtc.rightTuples}, tc.atc.expected, unorderedVerifier,
			func(inputs []colexecbase.Operator) (colexecbase.Operator, error) {
				spec := MakeHashJoinerSpec(
					tc.jtc.joinType,
					tc.jtc.leftEqCols,
					tc.jtc.rightEqCols,
					tc.jtc.leftTypes,
					tc.jtc.rightTypes,
					tc.jtc.rightEqColsAreKey,
				)
				return newHashGroupJoiner(
					testAllocator, spec,
					inputs[0], inputs[1],
					tc.joinOutProjection,
					&colexecagg.NewAggregatorArgs{
						Allocator:      testAllocator,
						MemAccount:     testMemAcc,
						InputTypes:     tc.atc.typs,
						Spec:           tc.atc.spec,
						EvalCtx:        &evalCtx,
						Constructors:   constructors,
						ConstArguments: constArguments,
						OutputTypes:    outputTypes,
					},
				), nil
			})
	}
}
