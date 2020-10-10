// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// joinReaderState represents the state of the processor.
type joinReaderState int

const (
	jrStateUnknown joinReaderState = iota
	// jrReadingInput means that a batch of rows is being read from the input.
	jrReadingInput
	// jrPerformingLookup means we are performing an index lookup for the current
	// input row batch.
	jrPerformingLookup
	// jrEmittingRows means we are emitting the results of the index lookup.
	jrEmittingRows
	// jrReadyToDrain means we are done but have not yet started draining.
	jrReadyToDrain
)

// joinReaderType represents the type of join being used.
type joinReaderType int

const (
	// lookupJoinReaderType means we are performing a lookup join.
	lookupJoinReaderType joinReaderType = iota
	// indexJoinReaderType means we are performing an index join.
	indexJoinReaderType
)

// joinReader performs a lookup join between `input` and the specified `index`.
// `lookupCols` specifies the input columns which will be used for the index
// lookup.
type joinReader struct {
	joinerBase
	strategy joinReaderStrategy

	// runningState represents the state of the joinReader. This is in addition to
	// ProcessorBase.State - the runningState is only relevant when
	// ProcessorBase.State == StateRunning.
	runningState joinReaderState

	diskMonitor *mon.BytesMonitor

	desc             tabledesc.Immutable
	index            *descpb.IndexDescriptor
	colIdxMap        map[descpb.ColumnID]int
	maintainOrdering bool

	// fetcher wraps the row.Fetcher used to perform lookups. This enables the
	// joinReader to wrap the fetcher with a stat collector when necessary.
	fetcher            rowFetcher
	alloc              rowenc.DatumAlloc
	rowAlloc           rowenc.EncDatumRowAlloc
	shouldLimitBatches bool
	readerType         joinReaderType

	input      execinfra.RowSource
	inputTypes []*types.T
	// Column indexes in the input stream specifying the columns which match with
	// the index columns. These are the equality columns of the join.
	lookupCols []uint32

	// Batch size for fetches. Not a constant so we can lower for testing.
	batchSizeBytes    int64
	curBatchSizeBytes int64

	// rowsRead is the total number of rows that this fetcher read from
	// disk.
	rowsRead int64

	// State variables for each batch of input rows.
	scratchInputRows rowenc.EncDatumRows

	// Fields used when this is the second join in a pair of joins that are
	// together implementing left {outer,semi,anti} joins where the first join
	// produces false positives because it cannot evaluate the whole expression
	// (or evaluate it accurately, as is sometimes the case with inverted
	// indexes). The first join is running a left outer or inner join, and each
	// group of rows seen by the second join correspond to one left row.
	//
	// TODO(sumeer): add support for joinReader to also be the first
	// join in this pair, say when the index can evaluate most of the
	// join condition (the part with low selectivity), but not all.
	// Currently only the invertedJoiner can serve as the first join
	// in the pair.

	// The input rows in the current batch belong to groups which are tracked in
	// groupingState. The last row from the last batch is in
	// lastInputRowFromLastBatch -- it is tracked because we don't know if it
	// was the last row in a group until we get to the next batch. NB:
	// groupingState is used even when there is no grouping -- we simply have
	// groups of one.
	groupingState *inputBatchGroupingState

	lastBatchState struct {
		lastInputRow       rowenc.EncDatumRow
		lastGroupMatched   bool
		lastGroupContinued bool
	}
}

var _ execinfra.Processor = &joinReader{}
var _ execinfra.RowSource = &joinReader{}
var _ execinfrapb.MetadataSource = &joinReader{}
var _ execinfra.OpNode = &joinReader{}
var _ execinfra.IOReader = &joinReader{}

const joinReaderProcName = "join reader"

// newJoinReader returns a new joinReader.
func newJoinReader(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.JoinReaderSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
	readerType joinReaderType,
) (execinfra.RowSourcedProcessor, error) {
	if spec.IndexIdx != 0 && readerType == indexJoinReaderType {
		return nil, errors.AssertionFailedf("index join must be against primary index")
	}

	var lookupCols []uint32
	switch readerType {
	case indexJoinReaderType:
		pkIDs := spec.Table.PrimaryIndex.ColumnIDs
		lookupCols = make([]uint32, len(pkIDs))
		for i := range pkIDs {
			lookupCols[i] = uint32(i)
		}
	case lookupJoinReaderType:
		lookupCols = spec.LookupColumns
	default:
		return nil, errors.Errorf("unsupported joinReaderType")
	}
	jr := &joinReader{
		desc:             tabledesc.MakeImmutable(spec.Table),
		maintainOrdering: spec.MaintainOrdering,
		input:            input,
		inputTypes:       input.OutputTypes(),
		lookupCols:       lookupCols,
	}
	if readerType != indexJoinReaderType {
		// TODO(sumeer): When LeftJoinWithPairedJoiner, the lookup columns and the
		// bool column must be projected away by the optimizer after this join.
		jr.groupingState = &inputBatchGroupingState{doGrouping: spec.LeftJoinWithPairedJoiner}
	}
	var err error
	var isSecondary bool
	jr.index, isSecondary, err = jr.desc.FindIndexByIndexIdx(int(spec.IndexIdx))
	if err != nil {
		return nil, err
	}
	returnMutations := spec.Visibility == execinfra.ScanVisibilityPublicAndNotPublic
	jr.colIdxMap = jr.desc.ColumnIdxMapWithMutations(returnMutations)

	columnIDs, _ := jr.index.FullColumnIDs()
	indexCols := make([]uint32, len(columnIDs))
	columnTypes := jr.desc.ColumnTypesWithMutations(returnMutations)
	for i, columnID := range columnIDs {
		indexCols[i] = uint32(columnID)
	}

	// If the lookup columns form a key, there is only one result per lookup, so the fetcher
	// should parallelize the key lookups it performs.
	jr.shouldLimitBatches = !spec.LookupColumnsAreKey && readerType == lookupJoinReaderType
	jr.readerType = readerType

	// Add all requested system columns to the output.
	var sysColDescs []descpb.ColumnDescriptor
	if spec.HasSystemColumns {
		sysColDescs = colinfo.AllSystemColumnDescs
	}
	for i := range sysColDescs {
		columnTypes = append(columnTypes, sysColDescs[i].Type)
		jr.colIdxMap[sysColDescs[i].ID] = len(jr.colIdxMap)
	}

	var leftTypes []*types.T
	var leftEqCols []uint32
	switch readerType {
	case indexJoinReaderType:
		// Index join performs a join between a secondary index, the `input`,
		// and the primary index of the same table, `desc`, to retrieve columns
		// which are not stored in the secondary index. It outputs the looked
		// up rows as is (meaning that the output rows before post-processing
		// will contain all columns from the table) whereas the columns that
		// came from the secondary index (input rows) are ignored. As a result,
		// we leave leftTypes as empty.
		leftEqCols = indexCols
	case lookupJoinReaderType:
		leftTypes = input.OutputTypes()
		leftEqCols = jr.lookupCols
	default:
		return nil, errors.Errorf("unsupported joinReaderType")
	}

	if err := jr.joinerBase.init(
		jr,
		flowCtx,
		processorID,
		leftTypes,
		columnTypes,
		spec.Type,
		spec.OnExpr,
		leftEqCols,
		indexCols,
		post,
		output,
		execinfra.ProcStateOpts{
			InputsToDrain: []execinfra.RowSource{jr.input},
			TrailingMetaCallback: func(ctx context.Context) []execinfrapb.ProducerMetadata {
				jr.close()
				return jr.generateMeta(ctx)
			},
		},
	); err != nil {
		return nil, err
	}

	collectingStats := false
	if sp := opentracing.SpanFromContext(flowCtx.EvalCtx.Ctx()); sp != nil && tracing.IsRecording(sp) {
		collectingStats = true
	}

	neededRightCols := jr.neededRightCols()
	if isSecondary && !neededRightCols.SubsetOf(getIndexColSet(jr.index, jr.colIdxMap)) {
		return nil, errors.Errorf("joinreader index does not cover all columns")
	}

	var fetcher row.Fetcher
	var rightCols util.FastIntSet
	switch readerType {
	case indexJoinReaderType:
		rightCols = jr.Out.NeededColumns()
	case lookupJoinReaderType:
		rightCols = neededRightCols
	default:
		return nil, errors.Errorf("unsupported joinReaderType")
	}

	_, _, err = initRowFetcher(
		flowCtx, &fetcher, &jr.desc, int(spec.IndexIdx), jr.colIdxMap, false, /* reverse */
		rightCols, false /* isCheck */, jr.EvalCtx.Mon, &jr.alloc, spec.Visibility, spec.LockingStrength, //nolint:monitor
		spec.LockingWaitPolicy, sysColDescs,
	)

	if err != nil {
		return nil, err
	}
	if collectingStats {
		jr.input = newInputStatCollector(jr.input)
		jr.fetcher = newRowFetcherStatCollector(&fetcher)
		jr.FinishTrace = jr.outputStatsToTrace
	} else {
		jr.fetcher = &fetcher
	}

	jr.initJoinReaderStrategy(flowCtx, columnTypes, len(columnIDs), rightCols, readerType)
	jr.batchSizeBytes = jr.strategy.getLookupRowsBatchSizeHint()

	// TODO(radu): verify the input types match the index key types
	return jr, nil
}

func (jr *joinReader) initJoinReaderStrategy(
	flowCtx *execinfra.FlowCtx,
	typs []*types.T,
	numKeyCols int,
	neededRightCols util.FastIntSet,
	readerType joinReaderType,
) {
	spanBuilder := span.MakeBuilder(flowCtx.Codec(), &jr.desc, jr.index)
	spanBuilder.SetNeededColumns(neededRightCols)

	var keyToInputRowIndices map[string][]int
	if readerType != indexJoinReaderType {
		keyToInputRowIndices = make(map[string][]int)
	}
	// Else: see the comment in defaultSpanGenerator on why we don't need
	// this map for index joins.
	spanGenerator := defaultSpanGenerator{
		spanBuilder:          spanBuilder,
		keyToInputRowIndices: keyToInputRowIndices,
		numKeyCols:           numKeyCols,
		lookupCols:           jr.lookupCols,
	}
	if readerType == indexJoinReaderType {
		jr.strategy = &joinReaderIndexJoinStrategy{
			joinerBase:           &jr.joinerBase,
			defaultSpanGenerator: spanGenerator,
		}
		return
	}

	if !jr.maintainOrdering {
		jr.strategy = &joinReaderNoOrderingStrategy{
			joinerBase:           &jr.joinerBase,
			defaultSpanGenerator: spanGenerator,
			isPartialJoin:        jr.joinType == descpb.LeftSemiJoin || jr.joinType == descpb.LeftAntiJoin,
			groupingState:        jr.groupingState,
		}
		return
	}

	ctx := flowCtx.EvalCtx.Ctx()
	// Limit the memory use by creating a child monitor with a hard limit.
	// joinReader will overflow to disk if this limit is not enough.
	limit := execinfra.GetWorkMemLimit(flowCtx.Cfg)
	if flowCtx.Cfg.TestingKnobs.ForceDiskSpill {
		limit = 1
	}
	// Initialize memory monitors and row container for looked up rows.
	jr.MemMonitor = execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, "joinreader-limited") //nolint:monitor
	jr.diskMonitor = execinfra.NewMonitor(ctx, flowCtx.Cfg.DiskMonitor, "joinreader-disk")
	drc := rowcontainer.NewDiskBackedNumberedRowContainer(
		false, /* deDup */
		typs,
		jr.EvalCtx,
		jr.FlowCtx.Cfg.TempStorage,
		jr.MemMonitor,
		jr.diskMonitor,
	)
	if limit < mon.DefaultPoolAllocationSize {
		// The memory limit is too low for caching, most likely to force disk
		// spilling for testing.
		drc.DisableCache = true
	}
	jr.strategy = &joinReaderOrderingStrategy{
		joinerBase:           &jr.joinerBase,
		defaultSpanGenerator: spanGenerator,
		isPartialJoin:        jr.joinType == descpb.LeftSemiJoin || jr.joinType == descpb.LeftAntiJoin,
		lookedUpRows:         drc,
		groupingState:        jr.groupingState,
	}
}

// getIndexColSet returns a set of all column indices for the given index.
func getIndexColSet(
	index *descpb.IndexDescriptor, colIdxMap map[descpb.ColumnID]int,
) util.FastIntSet {
	cols := util.MakeFastIntSet()
	err := index.RunOverAllColumns(func(id descpb.ColumnID) error {
		cols.Add(colIdxMap[id])
		return nil
	})
	if err != nil {
		// This path should never be hit since the column function never returns an
		// error.
		panic(err)
	}
	return cols
}

// SetBatchSizeBytes sets the desired batch size. It should only be used in tests.
func (jr *joinReader) SetBatchSizeBytes(batchSize int64) {
	jr.batchSizeBytes = batchSize
}

// Spilled returns whether the joinReader spilled to disk.
func (jr *joinReader) Spilled() bool {
	return jr.strategy.spilled()
}

// neededRightCols returns the set of column indices which need to be fetched
// from the right side of the join (jr.desc).
func (jr *joinReader) neededRightCols() util.FastIntSet {
	neededCols := jr.Out.NeededColumns()

	// Get the columns from the right side of the join and shift them over by
	// the size of the left side so the right side starts at 0.
	neededRightCols := util.MakeFastIntSet()
	for i, ok := neededCols.Next(len(jr.inputTypes)); ok; i, ok = neededCols.Next(i + 1) {
		neededRightCols.Add(i - len(jr.inputTypes))
	}

	// Add columns needed by OnExpr.
	for _, v := range jr.onCond.Vars.GetIndexedVars() {
		rightIdx := v.Idx - len(jr.inputTypes)
		if rightIdx >= 0 {
			neededRightCols.Add(rightIdx)
		}
	}

	return neededRightCols
}

// Next is part of the RowSource interface.
func (jr *joinReader) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// The lookup join is implemented as follows:
	// - Read the input rows in batches.
	// - For each batch, map the rows onto index keys and perform an index
	//   lookup for those keys. Note that multiple rows may map to the same key.
	// - Retrieve the index lookup results in batches, since the index scan may
	//   return more rows than the input batch size.
	// - Join the index rows with the corresponding input rows and buffer the
	//   results in jr.toEmit.
	for jr.State == execinfra.StateRunning {
		var row rowenc.EncDatumRow
		var meta *execinfrapb.ProducerMetadata
		switch jr.runningState {
		case jrReadingInput:
			jr.runningState, row, meta = jr.readInput()
		case jrPerformingLookup:
			jr.runningState, meta = jr.performLookup()
		case jrEmittingRows:
			jr.runningState, row, meta = jr.emitRow()
		case jrReadyToDrain:
			jr.MoveToDraining(nil)
			meta = jr.DrainHelper()
			jr.runningState = jrStateUnknown
		default:
			log.Fatalf(jr.Ctx, "unsupported state: %d", jr.runningState)
		}
		if row == nil && meta == nil {
			continue
		}
		if meta != nil {
			return nil, meta
		}
		if outRow := jr.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, jr.DrainHelper()
}

// readInput reads the next batch of input rows and starts an index scan.
// It can sometimes emit a single row on behalf of the previous batch.
func (jr *joinReader) readInput() (
	joinReaderState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	if jr.groupingState != nil {
		// Lookup join.
		if jr.groupingState.initialized {
			// State is from last batch.
			jr.lastBatchState.lastGroupMatched = jr.groupingState.lastGroupMatched()
			jr.groupingState.reset()
			jr.lastBatchState.lastGroupContinued = false
		}
		// Else, returning meta interrupted reading the input batch, so we already
		// did the reset for this batch.
	}
	// Read the next batch of input rows.
	for jr.curBatchSizeBytes < jr.batchSizeBytes {
		row, meta := jr.input.Next()
		if meta != nil {
			if meta.Err != nil {
				jr.MoveToDraining(nil /* err */)
				return jrStateUnknown, nil, meta
			}
			return jrReadingInput, nil, meta
		}
		if row == nil {
			break
		}
		jr.curBatchSizeBytes += int64(row.Size())
		if jr.groupingState != nil {
			// Lookup Join.
			if err := jr.processContinuationValForRow(row); err != nil {
				jr.MoveToDraining(err)
				return jrStateUnknown, nil, jr.DrainHelper()
			}
		}
		jr.scratchInputRows = append(jr.scratchInputRows, jr.rowAlloc.CopyRow(row))
	}
	var outRow rowenc.EncDatumRow
	// Finished reading the input batch.
	if jr.groupingState != nil {
		// Lookup join.
		outRow = jr.allContinuationValsProcessed()
	}

	if len(jr.scratchInputRows) == 0 {
		log.VEventf(jr.Ctx, 1, "no more input rows")
		if outRow != nil {
			return jrReadyToDrain, outRow, nil
		}
		// We're done.
		jr.MoveToDraining(nil)
		return jrStateUnknown, nil, jr.DrainHelper()
	}
	log.VEventf(jr.Ctx, 1, "read %d input rows", len(jr.scratchInputRows))

	if jr.groupingState != nil && len(jr.scratchInputRows) > 0 {
		jr.updateGroupingStateForNonEmptyBatch()
	}

	spans, err := jr.strategy.processLookupRows(jr.scratchInputRows)
	if err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}
	jr.scratchInputRows = jr.scratchInputRows[:0]
	jr.curBatchSizeBytes = 0
	if len(spans) == 0 {
		// All of the input rows were filtered out. Skip the index lookup.
		return jrEmittingRows, outRow, nil
	}

	// Sort the spans for the following cases:
	// - For lookupJoinReaderType: this is so that we can rely upon the fetcher
	//   to limit the number of results per batch. It's safe to reorder the
	//   spans here because we already restore the original order of the output
	//   during the output collection phase.
	// - For indexJoinReaderType when !maintainOrdering: this allows lower
	//   layers to optimize iteration over the data. Note that the looked up
	//   rows are output unchanged, in the retrieval order, so it is not safe to
	//   do this when maintainOrdering is true (the ordering to be maintained
	//   may be different than the ordering in the index).
	if jr.readerType == lookupJoinReaderType ||
		(jr.readerType == indexJoinReaderType && !jr.maintainOrdering) {
		sort.Sort(spans)
	}

	log.VEventf(jr.Ctx, 1, "scanning %d spans", len(spans))
	if err := jr.fetcher.StartScan(
		jr.Ctx, jr.FlowCtx.Txn, spans, jr.shouldLimitBatches, 0, /* limitHint */
		jr.FlowCtx.TraceKV); err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}

	return jrPerformingLookup, outRow, nil
}

// performLookup reads the next batch of index rows.
func (jr *joinReader) performLookup() (joinReaderState, *execinfrapb.ProducerMetadata) {
	nCols := len(jr.lookupCols)

	for {
		// Construct a "partial key" of nCols, so we can match the key format that
		// was stored in our keyToInputRowIndices map. This matches the format that
		// is output in jr.generateSpan.
		var key roachpb.Key
		// Index joins do not look at this key parameter so don't bother populating
		// it, since it is not cheap for long keys.
		if jr.readerType != indexJoinReaderType {
			var err error
			key, err = jr.fetcher.PartialKey(nCols)
			if err != nil {
				jr.MoveToDraining(err)
				return jrStateUnknown, jr.DrainHelper()
			}
		}

		// Fetch the next row and copy it into the row container.
		lookedUpRow, _, _, err := jr.fetcher.NextRow(jr.Ctx)
		if err != nil {
			jr.MoveToDraining(scrub.UnwrapScrubError(err))
			return jrStateUnknown, jr.DrainHelper()
		}
		if lookedUpRow == nil {
			// Done with this input batch.
			break
		}
		jr.rowsRead++

		if nextState, err := jr.strategy.processLookedUpRow(jr.Ctx, lookedUpRow, key); err != nil {
			jr.MoveToDraining(err)
			return jrStateUnknown, jr.DrainHelper()
		} else if nextState != jrPerformingLookup {
			return nextState, nil
		}
	}
	log.VEvent(jr.Ctx, 1, "done joining rows")
	jr.strategy.prepareToEmit(jr.Ctx)

	return jrEmittingRows, nil
}

// emitRow returns the next row from jr.toEmit, if present. Otherwise it
// prepares for another input batch.
func (jr *joinReader) emitRow() (
	joinReaderState,
	rowenc.EncDatumRow,
	*execinfrapb.ProducerMetadata,
) {
	rowToEmit, nextState, err := jr.strategy.nextRowToEmit(jr.Ctx)
	if err != nil {
		jr.MoveToDraining(err)
		return jrStateUnknown, nil, jr.DrainHelper()
	}
	return nextState, rowToEmit, nil
}

// Start is part of the RowSource interface.
func (jr *joinReader) Start(ctx context.Context) context.Context {
	jr.input.Start(ctx)
	ctx = jr.StartInternal(ctx, joinReaderProcName)
	jr.runningState = jrReadingInput
	return ctx
}

// ConsumerClosed is part of the RowSource interface.
func (jr *joinReader) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	jr.close()
}

func (jr *joinReader) close() {
	if jr.InternalClose() {
		if jr.fetcher != nil {
			jr.fetcher.Close(jr.Ctx)
		}
		jr.strategy.close(jr.Ctx)
		if jr.MemMonitor != nil {
			jr.MemMonitor.Stop(jr.Ctx)
		}
		if jr.diskMonitor != nil {
			jr.diskMonitor.Stop(jr.Ctx)
		}
	}
}

var _ execinfrapb.DistSQLSpanStats = &JoinReaderStats{}

const joinReaderTagPrefix = "joinreader."

// Stats implements the SpanStats interface.
func (jrs *JoinReaderStats) Stats() map[string]string {
	statsMap := jrs.InputStats.Stats(joinReaderTagPrefix)
	toMerge := jrs.IndexLookupStats.Stats(joinReaderTagPrefix + "index.")
	for k, v := range toMerge {
		statsMap[k] = v
	}
	return statsMap
}

// StatsForQueryPlan implements the DistSQLSpanStats interface.
func (jrs *JoinReaderStats) StatsForQueryPlan() []string {
	is := append(
		jrs.InputStats.StatsForQueryPlan(""),
		jrs.IndexLookupStats.StatsForQueryPlan("index ")...,
	)
	return is
}

// outputStatsToTrace outputs the collected joinReader stats to the trace. Will
// fail silently if the joinReader is not collecting stats.
func (jr *joinReader) outputStatsToTrace() {
	is, ok := getInputStats(jr.FlowCtx, jr.input)
	if !ok {
		return
	}
	ils, ok := getFetcherInputStats(jr.FlowCtx, jr.fetcher)
	if !ok {
		return
	}

	// TODO(asubiotto): Add memory and disk usage to EXPLAIN ANALYZE.
	jrs := &JoinReaderStats{
		InputStats:       is,
		IndexLookupStats: ils,
	}
	if sp := opentracing.SpanFromContext(jr.Ctx); sp != nil {
		tracing.SetSpanStats(sp, jrs)
	}
}

// GetBytesRead is part of the execinfra.IOReader interface.
func (jr *joinReader) GetBytesRead() int64 {
	return jr.fetcher.GetBytesRead()
}

// GetRowsRead is part of the execinfra.IOReader interface.
func (jr *joinReader) GetRowsRead() int64 {
	return jr.rowsRead
}

func (jr *joinReader) generateMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	trailingMeta := make([]execinfrapb.ProducerMetadata, 1)
	meta := &trailingMeta[0]
	meta.Metrics = execinfrapb.GetMetricsMeta()
	meta.Metrics.RowsRead = jr.GetRowsRead()
	meta.Metrics.BytesRead = jr.GetBytesRead()
	if tfs := execinfra.GetLeafTxnFinalState(ctx, jr.FlowCtx.Txn); tfs != nil {
		trailingMeta = append(trailingMeta,
			execinfrapb.ProducerMetadata{LeafTxnFinalState: tfs},
		)
	}
	return trailingMeta
}

// DrainMeta is part of the MetadataSource interface.
func (jr *joinReader) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	return jr.generateMeta(ctx)
}

// ChildCount is part of the execinfra.OpNode interface.
func (jr *joinReader) ChildCount(verbose bool) int {
	if _, ok := jr.input.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the execinfra.OpNode interface.
func (jr *joinReader) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		if n, ok := jr.input.(execinfra.OpNode); ok {
			return n
		}
		panic("input to joinReader is not an execinfra.OpNode")
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

// processContinuationValForRow is called for each row in a batch which has a
// continuation column.
func (jr *joinReader) processContinuationValForRow(row rowenc.EncDatumRow) error {
	if !jr.groupingState.doGrouping {
		// Lookup join with no continuation column.
		jr.groupingState.addContinuationValForRow(false)
	} else {
		continuationEncDatum := row[len(row)-1]
		if err := continuationEncDatum.EnsureDecoded(types.Bool, &jr.alloc); err != nil {
			return err
		}
		continuationVal := bool(*continuationEncDatum.Datum.(*tree.DBool))
		jr.groupingState.addContinuationValForRow(continuationVal)
		if len(jr.scratchInputRows) == 0 && continuationVal {
			// First row in batch is a continuation of last group.
			jr.lastBatchState.lastGroupContinued = true
		}
	}
	return nil
}

// allContinuationValsProcessed is called after all the rows in the batch have
// been read, or the batch is empty, and processContinuationValForRow has been
// called. It returns a non-nil row if one needs to output a row from the
// batch previous to the current batch.
func (jr *joinReader) allContinuationValsProcessed() rowenc.EncDatumRow {
	var outRow rowenc.EncDatumRow
	jr.groupingState.initialized = true
	if jr.lastBatchState.lastInputRow != nil && !jr.lastBatchState.lastGroupContinued {
		// Group ended in previous batch and this is a lookup join with a
		// continuation column.
		if !jr.lastBatchState.lastGroupMatched {
			// Handle the cases where we need to emit the left row when there is no
			// match.
			switch jr.joinType {
			case descpb.LeftOuterJoin:
				outRow = jr.renderUnmatchedRow(jr.lastBatchState.lastInputRow, leftSide)
			case descpb.LeftAntiJoin:
				outRow = jr.lastBatchState.lastInputRow
			}
		}
		// Else the last group matched, so already emitted 1+ row for left outer
		// join, 1 row for semi join, and no need to emit for anti join.
	}
	// Else, last group continued, or this is the first ever batch, or all
	// groups are of length 1. Either way, we don't need to do anything
	// special for the last group from the last batch.

	jr.lastBatchState.lastInputRow = nil
	return outRow
}

// updateGroupingStateForNonEmptyBatch is called once the batch has been read
// and found to be non-empty.
func (jr *joinReader) updateGroupingStateForNonEmptyBatch() {
	if jr.groupingState.doGrouping {
		// Groups can continue from one batch to another.

		// Remember state from the last group in this batch.
		jr.lastBatchState.lastInputRow = jr.scratchInputRows[len(jr.scratchInputRows)-1]
		// Initialize matching state for the first group in this batch.
		if jr.lastBatchState.lastGroupMatched && jr.lastBatchState.lastGroupContinued {
			jr.groupingState.setFirstGroupMatched()
		}
	}
}

// inputBatchGroupingState encapsulates the state needed for all the
// groups in an input batch, for lookup joins (not used for index
// joins).
// It functions in one of two modes:
// - doGrouping is false: It is expected that for each input row in
//   a batch, addContinuationValForRow(false) will be called.
// - doGrouping is true: The join is functioning in a manner where
//   the continuation column in the input indicates the parameter
//   value of addContinuationValForRow calls.
//
// The initialization and resetting of state for a batch is
// handled by joinReader. Updates to this state based on row
// matching is done by the appropriate joinReaderStrategy
// implementation. The joinReaderStrategy implementations
// also lookup the state to decide when to output.
type inputBatchGroupingState struct {
	doGrouping bool

	initialized bool
	// Row index in batch to the group index. Only used when doGrouping = true.
	batchRowToGroupIndex []int
	// State per group.
	groupState []groupState
}

type groupState struct {
	// Whether the group matched.
	matched bool
	// The last row index in the group. Only valid when doGrouping = true.
	lastRow int
}

func (ib *inputBatchGroupingState) reset() {
	ib.batchRowToGroupIndex = ib.batchRowToGroupIndex[:0]
	ib.groupState = ib.groupState[:0]
	ib.initialized = false
}

// addContinuationValForRow is called with each row in an input batch, with
// the cont parameter indicating whether or not it is a continuation of the
// group from the previous row.
func (ib *inputBatchGroupingState) addContinuationValForRow(cont bool) {
	if len(ib.groupState) == 0 || !cont {
		// First row in input batch or the start of a new group. We need to
		// add entries in the group indexed slices.
		ib.groupState = append(ib.groupState,
			groupState{matched: false, lastRow: len(ib.batchRowToGroupIndex)})
	}
	if ib.doGrouping {
		groupIndex := len(ib.groupState) - 1
		ib.groupState[groupIndex].lastRow = len(ib.batchRowToGroupIndex)
		ib.batchRowToGroupIndex = append(ib.batchRowToGroupIndex, groupIndex)
	}
}

func (ib *inputBatchGroupingState) setFirstGroupMatched() {
	ib.groupState[0].matched = true
}

func (ib *inputBatchGroupingState) setMatched(rowIndex int) {
	groupIndex := rowIndex
	if ib.doGrouping {
		groupIndex = ib.batchRowToGroupIndex[rowIndex]
	}
	ib.groupState[groupIndex].matched = true
}

func (ib *inputBatchGroupingState) getMatched(rowIndex int) bool {
	groupIndex := rowIndex
	if ib.doGrouping {
		groupIndex = ib.batchRowToGroupIndex[rowIndex]
	}
	return ib.groupState[groupIndex].matched
}

func (ib *inputBatchGroupingState) lastGroupMatched() bool {
	if !ib.doGrouping || len(ib.groupState) == 0 {
		return false
	}
	return ib.groupState[len(ib.groupState)-1].matched
}

func (ib *inputBatchGroupingState) isUnmatched(rowIndex int) bool {
	if !ib.doGrouping {
		// The rowIndex is also the groupIndex.
		return !ib.groupState[rowIndex].matched
	}
	groupIndex := ib.batchRowToGroupIndex[rowIndex]
	if groupIndex == len(ib.groupState)-1 {
		// Return false for last group since it is not necessarily complete yet --
		// the next batch may continue the group.
		return false
	}
	// Group is complete -- return true for the last row index in a group that
	// is unmatched. Note that there are join reader strategies that on a
	// row-by-row basis (a) evaluate the match condition, (b) decide whether to
	// output (including when there is no match). It is necessary to delay
	// saying that there is no match for the group until the last row in the
	// group since for earlier rows, when at step (b), one does not know the
	// match state of later rows in the group.
	return !ib.groupState[groupIndex].matched && ib.groupState[groupIndex].lastRow == rowIndex
}
