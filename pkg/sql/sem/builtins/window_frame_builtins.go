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

	"bytes"

	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/pkg/errors"
)

// MaybeReplaceWithFasterImplementation replaces aggregates with segment tree version, if present.
func MaybeReplaceWithFasterImplementation(
	windowFunc tree.WindowFunc, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) tree.WindowFunc {
	if framableAgg, ok := windowFunc.(*framableAggregateWindowFunc); ok {
		aggWindowFunc := framableAgg.agg.agg
		switch aggWindowFunc.(type) {
		case *MinAggregate:
			min := &minWindowFunc{&segmentTree{}}
			maxValue, _ := tree.NewDInt(0).Max(evalCtx)
			min.minTree.fanout = 2
			min.minTree.nilValue = maxValue
			min.minTree.op = minFunc
			min.minTree.build(evalCtx, wfr)
			return min
		case *MaxAggregate:
			max := &maxWindowFunc{&segmentTree{}}
			minValue, _ := tree.NewDInt(0).Min(evalCtx)
			max.maxTree.fanout = 2
			max.maxTree.nilValue = minValue
			max.maxTree.op = maxFunc
			max.maxTree.build(evalCtx, wfr)
			return max
		case *intSumAggregate:
			sum := &sumWindowFunc{&segmentTree{}}
			sum.initialize(evalCtx, wfr)
			return sum
		case *decimalSumAggregate:
			sum := &sumWindowFunc{&segmentTree{}}
			sum.initialize(evalCtx, wfr)
			return sum
		case *floatSumAggregate:
			sum := &sumWindowFunc{&segmentTree{}}
			sum.initialize(evalCtx, wfr)
			return sum
		case *avgAggregate:
			avg := &avgWindowFunc{&segmentTree{}}
			avg.sumTree.fanout = 2
			avg.setNilValue(wfr)
			avg.sumTree.op = sumFunc
			avg.sumTree.build(evalCtx, wfr)
			return avg
		}
	}
	return windowFunc
}

func (sum *sumWindowFunc) initialize(evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun) {
	sum.sumTree.fanout = 2
	sum.setNilValue(wfr)
	sum.sumTree.op = sumFunc
	sum.sumTree.build(evalCtx, wfr)
}

type segmentTree struct {
	fanout      int
	values      tree.Datums
	nilValue    tree.Datum
	leavesCount int
	op          func(*tree.EvalContext, tree.Datum, tree.Datum) (tree.Datum, error)
}

func (t *segmentTree) build(evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun) error {
	n := wfr.PartitionSize()
	treeSize, nodesOnNextLevel := 1, t.fanout
	for nodesOnNextLevel < n {
		treeSize += nodesOnNextLevel
		nodesOnNextLevel *= t.fanout
	}
	treeSize += nodesOnNextLevel
	t.leavesCount = nodesOnNextLevel
	t.values = make(tree.Datums, treeSize)
	return t.construct(evalCtx, wfr, 0, t.leavesCount, 0)
}

func (t *segmentTree) construct(
	evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun, l, r, cur int,
) error {
	if l+1 == r {
		if len(wfr.Args()) == 0 {
			return errors.Errorf("No value in a row during segment tree construction.")
		}
		if l >= wfr.PartitionSize() {
			t.values[cur] = t.nilValue
			return nil
		}
		t.values[cur] = wfr.ArgsByRowIdx(l)[0]
		return nil
	}
	step := (r - l) / t.fanout
	n := t.fanout
	if step == 0 {
		step = 1
		n = r - l
	}
	for i := 0; i < n; i++ {
		start, end := l+step*i, l+step*(i+1)
		if end >= r {
			end = r
		}
		err := t.construct(evalCtx, wfr, start, end, t.fanout*cur+i+1)
		if err != nil {
			return err
		}
	}
	result, err := t.op(evalCtx, t.values[t.fanout*cur+1], t.values[t.fanout*cur+2])
	if err != nil {
		return err
	}
	for i := 2; i < n; i++ {
		result, err = t.op(evalCtx, result, t.values[t.fanout*cur+i+1])
		if err != nil {
			return err
		}
	}
	t.values[cur] = result
	return err
}

func (t *segmentTree) query(
	evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	start, end := wfr.FrameStartIdx(), wfr.FrameEndIdx()
	if start >= end {
		return tree.DNull, nil
	}
	if start < 0 || end > wfr.PartitionSize() {
		return nil, errors.Errorf("Frame boundaries are incorrect while processing query on segment tree.")
	}
	return t.queryHelper(evalCtx, 0, t.leavesCount, start, end, 0)
}

func (t *segmentTree) queryHelper(
	evalCtx *tree.EvalContext, l, r, start, end, cur int,
) (tree.Datum, error) {
	if start <= l && end >= r {
		return t.values[cur], nil
	}
	if r <= start || l >= end {
		return t.nilValue, nil
	}
	step := (r - l) / t.fanout
	n := t.fanout
	if step == 0 {
		step = 1
		n = r - l
	}
	result, err := t.queryHelper(evalCtx, l, l+step, start, end, t.fanout*cur+1)
	if err != nil {
		return t.nilValue, err
	}
	for i := 1; i < n; i++ {
		tmp, err := t.queryHelper(evalCtx, l+step*i, l+step*(i+1), start, end, t.fanout*cur+i+1)
		if err != nil {
			return t.nilValue, err
		}
		result, err = t.op(evalCtx, result, tmp)
		if err != nil {
			return t.nilValue, err
		}
	}
	return result, err
}

func (t *segmentTree) toString() string {
	var buffer bytes.Buffer
	for idx := 0; idx < len(t.values); idx++ {
		buffer.WriteString(fmt.Sprintf("%+v: %+v\n", idx, t.values[idx]))
	}
	return buffer.String()
}

func sumFunc(evalCtx *tree.EvalContext, a, b tree.Datum) (tree.Datum, error) {
	if a == nil || a == tree.DNull {
		return b, nil
	}
	if b == nil || b == tree.DNull {
		return a, nil
	}
	switch ta := a.(type) {
	case *tree.DFloat:
		tb := b.(*tree.DFloat)
		return tree.NewDFloat(*ta + *tb), nil
	case *tree.DDecimal:
		var sum tree.DDecimal
		switch tb := b.(type) {
		case *tree.DDecimal:
			_, err := tree.ExactCtx.Add(&sum.Decimal, &ta.Decimal, &tb.Decimal)
			return &sum, err
		case *tree.DInt:
			// Overflow happened earlier.
			db := apd.Decimal{}
			db.SetCoefficient(int64(*tb))
			_, err := tree.ExactCtx.Add(&sum.Decimal, &ta.Decimal, &db)
			return &sum, err
		default:
			return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected summation of %s and %s", ta, tb)
		}
	case *tree.DInt:
		switch tb := b.(type) {
		case *tree.DDecimal:
			// Overflow happened earlier.
			var sum tree.DDecimal
			da := apd.Decimal{}
			da.SetCoefficient(int64(*ta))
			_, err := tree.ExactCtx.Add(&sum.Decimal, &da, &tb.Decimal)
			return &sum, err
		case *tree.DInt:
			ia, ib := int64(*ta), int64(*tb)
			sum, ok := arith.AddWithOverflow(ia, ib)
			if !ok {
				var sum tree.DDecimal
				da := apd.Decimal{}
				da.SetCoefficient(ia)
				db := apd.Decimal{}
				db.SetCoefficient(ib)
				_, err := tree.ExactCtx.Add(&sum.Decimal, &da, &db)
				return &sum, err
			}
			return tree.NewDInt(tree.DInt(sum)), nil
		default:
			return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected summation of %s and %s", ta, tb)
		}
	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected SUM result type: %s", ta)
	}
}

type sumWindowFunc struct {
	sumTree *segmentTree
}

func (w *sumWindowFunc) setNilValue(wfr *tree.WindowFrameRun) {
	if wfr.PartitionSize() > 0 && len(wfr.Args()) > 0 {
		switch wfr.ArgsByRowIdx(0)[0].(type) {
		case *tree.DFloat:
			w.sumTree.nilValue = tree.NewDFloat(0)
		case *tree.DDecimal:
			w.sumTree.nilValue = &tree.DDecimal{}
		case *tree.DInt:
			w.sumTree.nilValue = tree.NewDInt(0)
		default:
			panic("unexpected Nil Value type")
		}
	}
}

func (w *sumWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	return w.sumTree.query(evalCtx, wfr)
}

func (w *sumWindowFunc) Close(ctx context.Context, evalCtx *tree.EvalContext) {
	w.sumTree = nil
}

func maxFunc(evalCtx *tree.EvalContext, a, b tree.Datum) (tree.Datum, error) {
	if a.Compare(evalCtx, b) < 0 {
		return b, nil
	}
	return a, nil
}

type maxWindowFunc struct {
	maxTree *segmentTree
}

func (w *maxWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	return w.maxTree.query(evalCtx, wfr)
}

func (w *maxWindowFunc) Close(ctx context.Context, evalCtx *tree.EvalContext) {
	w.maxTree = nil
}

func minFunc(evalCtx *tree.EvalContext, a, b tree.Datum) (tree.Datum, error) {
	if a.Compare(evalCtx, b) > 0 {
		return b, nil
	}
	return a, nil
}

type minWindowFunc struct {
	minTree *segmentTree
}

func (w *minWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	return w.minTree.query(evalCtx, wfr)
}

func (w *minWindowFunc) Close(ctx context.Context, evalCtx *tree.EvalContext) {
	w.minTree = nil
}

type avgWindowFunc struct {
	sumTree *segmentTree
}

func (w *avgWindowFunc) setNilValue(wfr *tree.WindowFrameRun) {
	if wfr.PartitionSize() > 0 && len(wfr.Args()) > 0 {
		switch wfr.ArgsByRowIdx(0)[0].(type) {
		case *tree.DFloat:
			w.sumTree.nilValue = tree.NewDFloat(0)
		case *tree.DDecimal:
			w.sumTree.nilValue = &tree.DDecimal{}
		case *tree.DInt:
			w.sumTree.nilValue = tree.NewDInt(0)
		default:
			panic("unexpected Nil Value type")
		}
	}
}

func (w *avgWindowFunc) Compute(
	ctx context.Context, evalCtx *tree.EvalContext, wfr *tree.WindowFrameRun,
) (tree.Datum, error) {
	sum, err := w.sumTree.query(evalCtx, wfr)
	if err != nil {
		return nil, err
	}
	if sum == tree.DNull {
		return sum, nil
	}
	switch t := sum.(type) {
	case *tree.DFloat:
		return tree.NewDFloat(*t / tree.DFloat(wfr.FrameSize())), nil
	case *tree.DDecimal:
		var avg tree.DDecimal
		count := apd.New(int64(wfr.FrameSize()), 0)
		_, err := tree.DecimalCtx.Quo(&avg.Decimal, &t.Decimal, count)
		return &avg, err
	case *tree.DInt:
		dd := tree.DDecimal{}
		dd.SetCoefficient(int64(*t))
		var avg tree.DDecimal
		count := apd.New(int64(wfr.FrameSize()), 0)
		_, err := tree.DecimalCtx.Quo(&avg.Decimal, &dd.Decimal, count)
		return &avg, err
	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected SUM result type: %s", t)
	}
}

func (w *avgWindowFunc) Close(ctx context.Context, evalCtx *tree.EvalContext) {
	w.sumTree = nil
}
