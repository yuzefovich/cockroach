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

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type defaultCmpProjOp struct {
	projOpBase

	adapter             comparisonExprAdapter
	toDatumConverter    *colconv.VecToDatumConverter
	datumToVecConverter func(tree.Datum) interface{}
}

var _ colexecbase.Operator = &defaultCmpProjOp{}

func (d *defaultCmpProjOp) Init() {
	d.input.Init()
}

func (d *defaultCmpProjOp) Next(ctx context.Context) coldata.Batch {
	batch := d.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	sel := batch.Selection()
	output := batch.ColVec(d.outputIdx)
	d.allocator.PerformOperation([]coldata.Vec{output}, func() {
		d.toDatumConverter.ConvertBatchAndDeselect(batch)
		leftColumn := d.toDatumConverter.GetDatumColumn(d.col1Idx)
		rightColumn := d.toDatumConverter.GetDatumColumn(d.col2Idx)
		for i := 0; i < n; i++ {
			// Note that we performed a conversion with deselection, so there
			// is no need to check whether sel is non-nil.
			res, err := d.adapter.eval(leftColumn[i], rightColumn[i])
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			rowIdx := i
			if sel != nil {
				rowIdx = sel[i]
			}
			// Convert the datum into a physical type and write it out.
			// TODO(yuzefovich): this code block is repeated in several places.
			// Refactor it.
			if res == tree.DNull {
				output.Nulls().SetNull(rowIdx)
			} else {
				converted := d.datumToVecConverter(res)
				coldata.SetValueAt(output, converted, rowIdx)
			}
		}
	})
	// Although we didn't change the length of the batch, it is necessary to set
	// the length anyway (this helps maintaining the invariant of flat bytes).
	batch.SetLength(n)
	return batch
}

type defaultCmpRConstProjOp struct {
	projConstOpBase
	constArg tree.Datum

	adapter             comparisonExprAdapter
	toDatumConverter    *colconv.VecToDatumConverter
	datumToVecConverter func(tree.Datum) interface{}
}

var _ colexecbase.Operator = &defaultCmpRConstProjOp{}

func (d *defaultCmpRConstProjOp) Init() {
	d.input.Init()
}

func (d *defaultCmpRConstProjOp) Next(ctx context.Context) coldata.Batch {
	batch := d.input.Next(ctx)
	n := batch.Length()
	if n == 0 {
		return coldata.ZeroBatch
	}
	sel := batch.Selection()
	output := batch.ColVec(d.outputIdx)
	d.allocator.PerformOperation([]coldata.Vec{output}, func() {
		d.toDatumConverter.ConvertBatchAndDeselect(batch)
		nonConstColumn := d.toDatumConverter.GetDatumColumn(d.colIdx)
		for i := 0; i < n; i++ {
			// Note that we performed a conversion with deselection, so there
			// is no need to check whether sel is non-nil.
			res, err := d.adapter.eval(nonConstColumn[i], d.constArg)
			if err != nil {
				colexecerror.ExpectedError(err)
			}
			rowIdx := i
			if sel != nil {
				rowIdx = sel[i]
			}
			// Convert the datum into a physical type and write it out.
			// TODO(yuzefovich): this code block is repeated in several places.
			// Refactor it.
			if res == tree.DNull {
				output.Nulls().SetNull(rowIdx)
			} else {
				converted := d.datumToVecConverter(res)
				coldata.SetValueAt(output, converted, rowIdx)
			}
		}
	})
	// Although we didn't change the length of the batch, it is necessary to set
	// the length anyway (this helps maintaining the invariant of flat bytes).
	batch.SetLength(n)
	return batch
}
