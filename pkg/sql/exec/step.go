// Copyright 2019 The Cockroach Authors.
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

package exec

import "github.com/cockroachdb/cockroach/pkg/sql/exec/types"

// stepOp is an operator that implements STEP, returning only STEPth tuple
// (zeroth included).
type stepOp struct {
	input Operator

	internalBatch ColBatch
	step          uint64
	counter       uint64
}

// NewStepOp returns a new step operator with the given step.
func NewStepOp(input Operator, step uint64) Operator {
	c := &stepOp{
		input: input,
		step:  step,
	}
	return c
}

func (c *stepOp) Init() {
	c.internalBatch = NewMemBatch([]types.T{types.Int64})
	c.input.Init()
}

func (c *stepOp) Next() ColBatch {
	bat := c.input.Next()
	length := bat.Length()
	if length == 0 || c.step < 2 {
		return bat
	}
	idx := uint16(0)
	for idx == 0 {
		sel := bat.Selection()
		if sel != nil {
			sel = sel[idx:length]
			for _, i := range sel {
				if c.counter%c.step == 0 {
					sel[idx] = i
					idx++
				}
				c.counter++
			}
		} else {
			bat.SetSelection(true)
			sel = bat.Selection()
			for i := uint16(0); i < length; i++ {
				if c.counter%c.step == 0 {
					sel[idx] = i
					idx++
				}
				c.counter++
			}
		}
		if idx == 0 {
			bat := c.input.Next()
			length := bat.Length()
			if length == 0 || c.step < 2 {
				return bat
			}
		}
	}

	bat.SetLength(idx)
	return bat
}
