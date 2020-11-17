// size of LocalOnlySessionData is 216
// size of CmpOp is 72
// size of SelectClause is 168
// Code generated by execgen; DO NOT EDIT.
// Copyright 2018 The Cockroach Authors.
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = typeconv.DatumVecCanonicalTypeFamily
	_ apd.Context
	_ duration.Duration
	_ encoding.Direction
)

// EncDatumRowsToColVec converts one column from EncDatumRows to a column
// vector. columnIdx is the 0-based index of the column in the EncDatumRows.
func EncDatumRowsToColVec(
	allocator *colmem.Allocator,
	rows rowenc.EncDatumRows,
	vec coldata.Vec,
	columnIdx int,
	t *types.T,
	alloc *rowenc.DatumAlloc,
) error {
	var err error
	allocator.PerformOperation(
		[]coldata.Vec{vec},
		func() {
			switch t.Family() {
			case types.BoolFamily:
				switch t.Width() {
				case -1:
				default:
					col := vec.Bool()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = bool(*datum.(*tree.DBool))
							castV := v.(bool)
							col[i] = castV
						}
					}
				}
			case types.IntFamily:
				switch t.Width() {
				case 16:
					col := vec.Int16()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = int16(*datum.(*tree.DInt))
							castV := v.(int16)
							col[i] = castV
						}
					}
				case 32:
					col := vec.Int32()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = int32(*datum.(*tree.DInt))
							castV := v.(int32)
							col[i] = castV
						}
					}
				case -1:
				default:
					col := vec.Int64()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = int64(*datum.(*tree.DInt))
							castV := v.(int64)
							col[i] = castV
						}
					}
				}
			case types.FloatFamily:
				switch t.Width() {
				case -1:
				default:
					col := vec.Float64()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = float64(*datum.(*tree.DFloat))
							castV := v.(float64)
							col[i] = castV
						}
					}
				}
			case types.DecimalFamily:
				switch t.Width() {
				case -1:
				default:
					col := vec.Decimal()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = datum.(*tree.DDecimal).Decimal
							castV := v.(apd.Decimal)
							col[i].Set(&castV)
						}
					}
				}
			case types.DateFamily:
				switch t.Width() {
				case -1:
				default:
					col := vec.Int64()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = datum.(*tree.DDate).UnixEpochDaysWithOrig()
							castV := v.(int64)
							col[i] = castV
						}
					}
				}
			case types.TimestampFamily:
				switch t.Width() {
				case -1:
				default:
					col := vec.Timestamp()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = datum.(*tree.DTimestamp).Time
							castV := v.(time.Time)
							col[i] = castV
						}
					}
				}
			case types.IntervalFamily:
				switch t.Width() {
				case -1:
				default:
					col := vec.Interval()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = datum.(*tree.DInterval).Duration
							castV := v.(duration.Duration)
							col[i] = castV
						}
					}
				}
			case types.StringFamily:
				switch t.Width() {
				case -1:
				default:
					col := vec.Bytes()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {
							// Handle other STRING-related OID types, like oid.T_name.
							wrapper, ok := datum.(*tree.DOidWrapper)
							if ok {
								datum = wrapper.Wrapped
							}
							v = encoding.UnsafeConvertStringToBytes(string(*datum.(*tree.DString)))
							castV := v.([]byte)
							col.Set(i, castV)
						}
					}
				}
			case types.BytesFamily:
				switch t.Width() {
				case -1:
				default:
					col := vec.Bytes()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = encoding.UnsafeConvertStringToBytes(string(*datum.(*tree.DBytes)))
							castV := v.([]byte)
							col.Set(i, castV)
						}
					}
				}
			case types.TimestampTZFamily:
				switch t.Width() {
				case -1:
				default:
					col := vec.Timestamp()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = datum.(*tree.DTimestampTZ).Time
							castV := v.(time.Time)
							col[i] = castV
						}
					}
				}
			case types.OidFamily:
				switch t.Width() {
				case -1:
				default:
					col := vec.Int64()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = int64(datum.(*tree.DOid).DInt)
							castV := v.(int64)
							col[i] = castV
						}
					}
				}
			case types.UuidFamily:
				switch t.Width() {
				case -1:
				default:
					col := vec.Bytes()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = datum.(*tree.DUuid).UUID.GetBytesMut()
							castV := v.([]byte)
							col.Set(i, castV)
						}
					}
				}
			case typeconv.DatumVecCanonicalTypeFamily:
			default:
				switch t.Width() {
				case -1:
				default:
					col := vec.Datum()
					var v interface{}
					for i := range rows {
						row := rows[i]
						if row[columnIdx].Datum == nil {
							if err = row[columnIdx].EnsureDecoded(t, alloc); err != nil {
								return
							}
						}
						datum := row[columnIdx].Datum
						if datum == tree.DNull {
							vec.Nulls().SetNull(i)
						} else {

							v = datum
							castV := v.(tree.Datum)
							col.Set(i, castV)
						}
					}
				}
			}
		},
	)
	return err
}
