// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for span_encoder.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexecspan

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = rowenc.EncodeTableKey
	_ coldataext.Datum
)

// {{/*

// Declarations to make the template compile properly. These are template
// variables which are replaced during code generation.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily
const _TYPE_WIDTH = 0
const _IS_ASC = true

// _ASSIGN_SPAN_ENCODING is a template addition function for assigning the first
// input to the result of encoding the second input.
func _ASSIGN_SPAN_ENCODING(_, _ string) {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

// newSpanEncoder creates a new utility operator that, given input batches,
// generates the encoding for the given key column. It is used by SpanAssembler
// operators to generate spans for index joins and lookup joins.
func newSpanEncoder(
	allocator *colmem.Allocator, typ *types.T, asc bool, encodeColIdx int,
) spanEncoder {
	base := spanEncoderBase{
		allocator:    allocator,
		encodeColIdx: encodeColIdx,
	}
	switch asc {
	// {{range .}}
	case _IS_ASC:
		switch typeconv.TypeFamilyToCanonicalTypeFamily(typ.Family()) {
		// {{range .TypeFamilies}}
		case _CANONICAL_TYPE_FAMILY:
			switch typ.Width() {
			// {{range .Overloads}}
			case _TYPE_WIDTH:
				return &_OP_STRING{spanEncoderBase: base}
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
	colexecerror.InternalError(errors.AssertionFailedf("unsupported span encoder type %s", typ.Name()))
	return nil
}

type spanEncoder interface {
	// next generates the encoding for the current key column for each row int the
	// given batch, then returns each row's encoding as a value in a Bytes column.
	// The returned Bytes column is owned by the spanEncoder operator and should
	// not be modified. Calling next invalidates the results of previous calls to
	// next.
	next(batch coldata.Batch) *coldata.Bytes

	close()
}

type spanEncoderBase struct {
	allocator    *colmem.Allocator
	encodeColIdx int

	// outputBytes contains the encoding for each row of the key column. It is
	// reused between calls to next().
	outputBytes *coldata.Bytes

	// A scratch bytes slice used to hold each encoding before it is appended to
	// the output column. It is reused to avoid allocating for every row.
	scratch []byte
}

// {{range .}}
// {{range .TypeFamilies}}
// {{range .Overloads}}

type _OP_STRING struct {
	spanEncoderBase
}

var _ spanEncoder = &_OP_STRING{}

// next implements the spanEncoder interface.
func (op *_OP_STRING) next(batch coldata.Batch) *coldata.Bytes {
	n := batch.Length()
	if n == 0 {
		return nil
	}
	if op.outputBytes == nil {
		op.outputBytes = coldata.NewBytes(n)
	}
	op.outputBytes.ResetForAppend()
	oldBytesSize := op.outputBytes.Size()

	vec := batch.ColVec(op.encodeColIdx)
	col := vec.TemplateType()

	sel := batch.Selection()
	if sel != nil {
		if vec.Nulls().MaybeHasNulls() {
			nulls := vec.Nulls()
			for _, i := range sel {
				encodeSpan(true, true)
			}
		} else {
			for _, i := range sel {
				encodeSpan(true, false)
			}
		}
	} else {
		_ = col.Get(n - 1)
		if vec.Nulls().MaybeHasNulls() {
			nulls := vec.Nulls()
			for i := 0; i < n; i++ {
				encodeSpan(false, true)
			}
		} else {
			for i := 0; i < n; i++ {
				encodeSpan(false, false)
			}
		}
	}

	op.allocator.AdjustMemoryUsage(int64(op.outputBytes.Size() - oldBytesSize))
	return op.outputBytes
}

// {{end}}
// {{end}}
// {{end}}

// close implements the spanEncoder interface.
func (b *spanEncoderBase) close() {
	*b = spanEncoderBase{}
}

// execgen:inline
// execgen:template<hasSel, hasNulls>
func encodeSpan(hasSel bool, hasNulls bool) {
	op.scratch = op.scratch[:0]
	if hasNulls {
		if nulls.NullAt(i) {
			// {{if .Asc}}
			op.outputBytes.AppendVal(encoding.EncodeNullAscending(op.scratch))
			// {{else}}
			op.outputBytes.AppendVal(encoding.EncodeNullDescending(op.scratch))
			// {{end}}
			continue
		}
	}
	if !hasSel {
		// {{if .Sliceable}}
		//gcassert:bce
		// {{end}}
	}
	val := col.Get(i)
	_ASSIGN_SPAN_ENCODING(op.scratch, val)
	op.outputBytes.AppendVal(op.scratch)
}
