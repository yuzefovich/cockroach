// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package builtins

import (
	"context"
	"testing"

	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

const maxCount = 2000
const maxFanout = 16
const maxOffset = 100

func testSegmentTree(
	t *testing.T, count int, op func(*tree.EvalContext, tree.Datum, tree.Datum) (tree.Datum, error), fanout int,
) {
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	st := segmentTree{
		nilValue: tree.NewDInt(0),
		fanout:   fanout,
		op:       op,
	}
	wfr := makeTestWindowFrameRun(count)
	st.build(evalCtx, wfr)
	wfr.Frame = &tree.WindowFrame{tree.ROWS, tree.WindowFrameBounds{StartBound: &tree.WindowFrameBound{BoundType: tree.ValuePreceding}, EndBound: &tree.WindowFrameBound{BoundType: tree.ValueFollowing}}}
	for offset := 0; offset < maxOffset; offset += int(rand.Int31n(maxOffset / 10)) {
		wfr.StartBoundOffset = offset
		wfr.EndBoundOffset = offset

		for wfr.RowIdx = 0; wfr.RowIdx < count; wfr.RowIdx++ {
			queryResult, err := st.query(evalCtx, wfr)
			if err != nil {
				t.Errorf("Unexpected error received while testing segment tree: %+v", err)
			}
			stResult, _ := tree.AsDInt(queryResult)
			naiveResult := tree.DInt(0)
			for idx := wfr.RowIdx - offset; idx <= wfr.RowIdx+offset; idx++ {
				//t.Errorf("trying to add element at idx %+v\n", idx)
				if idx < 0 || idx >= wfr.PartitionSize() {
					continue
				}
				el, _ := tree.AsDInt(wfr.Rows[idx].Row[0])
				naiveResult += el
			}
			if stResult != naiveResult {
				t.Errorf("Segment tree returned wrong result: expected %+v, found %+v", naiveResult, stResult)
				t.Errorf("count: %+v idx: %+v offset: %+v", count, wfr.RowIdx, offset)
				t.Errorf(st.toString())
				panic("")
			}
		}
	}
}

func makeTestWindowFrameRun(count int) *tree.WindowFrameRun {
	return &tree.WindowFrameRun{
		Rows:        makeTestPartition(count),
		ArgIdxStart: 0,
		ArgCount:    1,
	}
}

func makeTestPartition(count int) []tree.IndexedRow {
	partition := make([]tree.IndexedRow, count)
	for idx := 0; idx < count; idx++ {
		partition[idx] = tree.IndexedRow{Idx: idx, Row: tree.Datums{tree.NewDInt(tree.DInt(rand.Int31n(10000)))}}
	}
	return partition
}

func TestSegmentTree(t *testing.T) {
	for fanout := 2; fanout <= maxFanout; fanout++ {
		for count := 1; count <= maxCount; count += int(rand.Int31n(maxCount / 5)) {
			testSegmentTree(t, count, sumFunc, fanout)
		}
	}
}
