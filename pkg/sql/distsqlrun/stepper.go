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

package distsqlrun

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/stringarena"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// Stepper is the physical processor implementation of the STEP operator.
type Stepper struct {
	ProcessorBase

	input      RowSource
	types      []sqlbase.ColumnType
	arena      stringarena.Arena
	seen       map[string]struct{}
	memAcc     mon.BoundAccount
	datumAlloc sqlbase.DatumAlloc
	scratch    []byte

	step    uint32
	counter uint32
}

var _ Processor = &Stepper{}
var _ RowSource = &Stepper{}

const stepProcName = "step"

// NewStepper instantiates a new Stepper processor.
func NewStepper(
	flowCtx *FlowCtx,
	processorID int32,
	spec *distsqlpb.StepperSpec,
	input RowSource,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (RowSourcedProcessor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := NewMonitor(ctx, flowCtx.EvalCtx.Mon, "stepper-mem")
	s := &Stepper{
		input:   input,
		memAcc:  memMonitor.MakeBoundAccount(),
		types:   input.OutputTypes(),
		step:    spec.Step,
		counter: 0,
	}

	var returnProcessor RowSourcedProcessor = s

	if err := s.Init(
		s, post, s.types, flowCtx, processorID, output, memMonitor, /* memMonitor */
		ProcStateOpts{
			InputsToDrain: []RowSource{s.input},
			TrailingMetaCallback: func(context.Context) []ProducerMetadata {
				s.close()
				return nil
			},
		}); err != nil {
		return nil, err
	}

	if sp := opentracing.SpanFromContext(ctx); sp != nil && tracing.IsRecording(sp) {
		s.input = NewInputStatCollector(s.input)
		s.finishTrace = s.outputStatsToTrace
	}

	return returnProcessor, nil
}

// Start is part of the RowSource interface.
func (s *Stepper) Start(ctx context.Context) context.Context {
	s.input.Start(ctx)
	return s.StartInternal(ctx, stepProcName)
}

func (s *Stepper) close() {
	if s.InternalClose() {
		s.memAcc.Close(s.Ctx)
		s.MemMonitor.Stop(s.Ctx)
	}
}

// Next is part of the RowSource interface.
func (s *Stepper) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	for s.State == StateRunning {
		row, meta := s.input.Next()
		if meta != nil {
			if meta.Err != nil {
				s.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			s.MoveToDraining(nil /* err */)
			break
		}

		if s.counter%s.step != 0 {
			// Stepping over this row.
			s.counter++
			continue
		}

		if outRow := s.ProcessRowHelper(row); outRow != nil {
			s.counter = 1
			return outRow, nil
		}
	}
	return nil, s.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (s *Stepper) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	s.close()
}

var _ distsqlpb.DistSQLSpanStats = &StepperStats{}

const stepperTagPrefix = "stepper."

// Stats implements the SpanStats interface.
func (ss *StepperStats) Stats() map[string]string {
	inputStatsMap := ss.InputStats.Stats(stepperTagPrefix)
	inputStatsMap[stepperTagPrefix+maxMemoryTagSuffix] = humanizeutil.IBytes(ss.MaxAllocatedMem)
	return inputStatsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (ss *StepperStats) StatsForQueryPlan() []string {
	return append(
		ss.InputStats.StatsForQueryPlan(""),
		fmt.Sprintf("%s: %s", maxMemoryQueryPlanSuffix, humanizeutil.IBytes(ss.MaxAllocatedMem)),
	)
}

// outputStatsToTrace outputs the collected distinct stats to the trace. Will
// fail silently if the Stepper processor is not collecting stats.
func (s *Stepper) outputStatsToTrace() {
	is, ok := getInputStats(s.flowCtx, s.input)
	if !ok {
		return
	}
	if sp := opentracing.SpanFromContext(s.Ctx); sp != nil {
		tracing.SetSpanStats(
			sp, &StepperStats{InputStats: is, MaxAllocatedMem: s.MemMonitor.MaximumBytes()},
		)
	}
}
