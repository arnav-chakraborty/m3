// Copyright (c) 2023 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"sort"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	pfs "github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/sharding" // Already present, but for context
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time" // Use xtime for m3 types like UnixNano

	"go.uber.org/zap"
)

const (
	rollupMarkerFileSuffix = "done"
	rollupFileSetTypeStr   = "rollup" // Used for directory naming if needed
)

// RollupProcessor is the interface for processing rollup rules.
type RollupProcessor interface {
	// Process processes rollup rules for a given namespace, shard, and block time.
	Process(ctx context.Context, ns namespace.Metadata, shardID uint32, blockTime xtime.UnixNano) error
	// NeedsRollup checks if a block needs to be rolled up for a given rule.
	NeedsRollup(ns namespace.Metadata, shardID uint32, blockTime xtime.UnixNano, rule retention.RollupRuleOptions) (bool, error)
}

type rollupProcessorOptions struct {
	clockOpts           clock.Options
	fsOpts              pfs.Options // Replaced placeholder
	seriesPool          series.Pool
	encoderPool         encoding.EncoderPool
	multiReaderIterPool encoding.MultiReaderIteratorPool
	iteratorPools       encoding.IteratorPools // Added for series iterators if needed
	identifierPool      ident.Pool
	logger              *zap.Logger
}

type rollupProcessor struct {
	opts    rollupProcessorOptions
	nowFn   func() time.Time // Kept as time.Time, consistent with clock.Options
	log     *zap.Logger
	fs      pfs.FileSystem // Added for convenience
	seriesBlockReadersPool *series.BlockReadersPool // For pooling block readers
}

// NewRollupProcessor creates a new rollup processor.
func NewRollupProcessor(opts rollupProcessorOptions) (RollupProcessor, error) {
	if opts.clockOpts == nil {
		opts.clockOpts = clock.NewOptions()
	}
	if opts.logger == nil {
		opts.logger = zap.NewNop()
	}
	if opts.fsOpts == nil {
		return nil, fmt.Errorf("filesystem options (fsOpts) cannot be nil")
	}
	if opts.multiReaderIterPool == nil {
		return nil, fmt.Errorf("multiReaderIterPool cannot be nil")
	}
	if opts.identifierPool == nil {
		return nil, fmt.Errorf("identifierPool cannot be nil")
	}
	if opts.encoderPool == nil {
		return nil, fmt.Errorf("encoderPool cannot be nil")
	}
	// iteratorPools might be optional depending on the chosen iteration strategy
	// if opts.iteratorPools == nil {
	// 	return nil, fmt.Errorf("iteratorPools cannot be nil")
	// }


	fs := pfs.NewFileSystem(opts.fsOpts) // Create a FileSystem instance

	return &rollupProcessor{
		opts:    opts,
		nowFn:   opts.clockOpts.NowFn(), // This returns time.Time, ensure compatibility or convert
		log:     opts.logger,
		fs:      fs,
		seriesBlockReadersPool: series.NewBlockReadersPool(nil), // Using default pool options for now
	}, nil
}

func (p *rollupProcessor) Process(ctx context.Context, ns namespace.Metadata, shardID uint32, blockTime xtime.UnixNano) error {
	nsIDStr := ns.ID().String()
	ropts := ns.Options().RetentionOptions()
	if ropts == nil {
		return fmt.Errorf("retention options not found for namespace: %s", nsIDStr)
	}

	rollupRules := ropts.RollupRules()
	if len(rollupRules) == 0 {
		p.log.Debug("no rollup rules for namespace", zap.String("namespace", nsIDStr))
		return nil
	}

	p.log.Info("Processing rollup rules",
		zap.String("namespace", nsIDStr),
		zap.Uint32("shard", shardID),
		zap.Time("blockTime", blockTime.ToTime()), // Convert xtime.UnixNano to time.Time for logging
		zap.Int("numRules", len(rollupRules)),
	)

	var lastErr error
	for _, rule := range rollupRules {
		shouldRollup, err := p.NeedsRollup(ns, shardID, blockTime, rule)
		if err != nil {
			p.log.Error("error checking NeedsRollup",
				zap.String("namespace", nsIDStr),
				zap.Uint32("shard", shardID),
				zap.Time("blockTime", blockTime.ToTime()),
				zap.String("resolution", rule.Resolution().String()),
				zap.Error(err),
			)
			lastErr = err // Continue processing other rules
			continue
		}

		if !shouldRollup {
			continue
		}

		p.log.Info("rollup required for rule",
			zap.String("namespace", nsIDStr),
			zap.Uint32("shard", shardID),
			zap.Time("blockTime", blockTime.ToTime()),
			zap.String("resolution", rule.Resolution().String()),
			zap.Duration("age", rule.Age()),
		)

		err = p.processRule(ctx, ns, shardID, blockTime, rule)
		if err != nil {
			p.log.Error("error processing rollup rule",
				zap.String("namespace", nsIDStr),
				zap.Uint32("shard", shardID),
				zap.Time("blockTime", blockTime.ToTime()),
				zap.String("resolution", rule.Resolution().String()),
				zap.Error(err),
			)
			lastErr = err // Capture and continue
		}
	}
	return lastErr
}

func (p *rollupProcessor) processRule(
	ctx context.Context,
	ns namespace.Metadata,
	shardID uint32,
	blockTime xtime.UnixNano,
	rule retention.RollupRuleOptions,
) error {
	nsIDStr := ns.ID().String()
	p.log.Info("starting processRule",
		zap.String("namespace", nsIDStr),
		zap.Uint32("shard", shardID),
		zap.Time("blockTime", blockTime.ToTime()),
		zap.String("targetResolution", rule.Resolution().String()))

	srcBlockPath := pfs.FilesetPathFromTime(
		p.opts.fsOpts.FilePathPrefix(),
		ns.ID(),
		shardID,
		blockTime,
		p.opts.fsOpts.InfoFilesPathFromTime(),
	)

	reader, err := pfs.NewReader(p.opts.fsOpts.BytesPool(), p.opts.fsOpts)
	if err != nil {
		return fmt.Errorf("failed to create fileset reader for source block %s: %w", srcBlockPath, err)
	}
	openOpts := pfs.ReaderOpenOptions{
		Identifier: pfs.FileSetFileIdentifier{
			Namespace:  ns.ID(),
			Shard:      shardID,
			BlockStart: blockTime,
		},
		FileSetType: persist.FileSetFlushType,
	}
	if err := reader.Open(openOpts); err != nil {
		return fmt.Errorf("failed to open fileset reader for source block %s: %w", srcBlockPath, err)
	}
	defer reader.Close()

	numSeries := reader.Entries()
	p.log.Info("opened source block for reading", zap.Int("numSeries", numSeries), zap.String("path", srcBlockPath))

	rollupFilesetDir := p.rollupFilesetPath(ns, shardID, blockTime, rule)
	if err := os.MkdirAll(rollupFilesetDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory for rollup fileset %s: %w", rollupFilesetDir, err)
	}

	tempRollupDir, err := os.MkdirTemp(p.opts.fsOpts.FilePathPrefix(), "temp_rollup_")
	if err != nil {
		return fmt.Errorf("failed to create temporary rollup directory: %w", err)
	}
	defer func() {
		if _, statErr := os.Stat(tempRollupDir); !os.IsNotExist(statErr) {
			os.RemoveAll(tempRollupDir)
		}
	}()

	tempFsOpts := p.opts.fsOpts.SetFilePathPrefix(tempRollupDir)
	writerOpenIdent := pfs.FileSetFileIdentifier{
		Namespace:  ns.ID(),
		Shard:      shardID,
		BlockStart: blockTime,
	}

	writer, err := pfs.NewWriter(tempFsOpts)
	if err != nil {
		return fmt.Errorf("failed to create new fileset writer: %w", err)
	}

	writerBlockSize := ns.Options().RetentionOptions().BlockSize()

	openWriterOpts := pfs.WriterOpenOptions{
		Identifier:       writerOpenIdent,
		BlockSize:        writerBlockSize,
		FileSetType:      persist.FileSetSnapshotType,
		WriterBufferSize: tempFsOpts.WriterBufferSize(),
	}

	if err := writer.Open(openWriterOpts); err != nil {
		return fmt.Errorf("failed to open fileset writer: %w", err)
	}

	var seriesCountWritten int
	var writeErrors []error

	for i := 0; i < numSeries; i++ {
		id, tagsData, data, _, readErr := reader.Read()
		if readErr != nil {
			p.log.Error("failed to read series from source block", zap.Int("seriesIndex", i), zap.Error(readErr))
			writeErrors = append(writeErrors, readErr)
			continue
		}

		clonedID := p.opts.identifierPool.Clone(id)
		// TODO: Clone tagsData if its underlying bytes are from a temporary buffer.

		aggregatedDps, aggErr := p.aggregateSeries(clonedID, data, blockTime, rule.Resolution(), ns) // Pass ns for schema
		if aggErr != nil {
			p.log.Error("failed to aggregate series", zap.Stringer("id", clonedID), zap.Error(aggErr))
			writeErrors = append(writeErrors, aggErr)
			clonedID.Finalize()
			continue
		}

		if len(aggregatedDps) == 0 {
			p.log.Debug("no aggregated data points for series", zap.Stringer("id", clonedID))
			clonedID.Finalize()
			continue
		}

		encoder := p.opts.encoderPool.Get()
		// TODO: Pass actual schema to Reset if available/necessary for the encoder
		encoder.Reset(blockTime, len(aggregatedDps), nil)

		for _, dp := range aggregatedDps {
			if encErr := encoder.Encode(dp, xtime.Second, nil); encErr != nil {
				p.log.Error("failed to encode datapoint", zap.Stringer("id", clonedID), zap.Error(encErr))
				writeErrors = append(writeErrors, encErr)
				goto seriesLoopEnd
			}
		}

		streamSegment, streamOk := encoder.Stream()
		if !streamOk || streamSegment == nil {
			err := fmt.Errorf("encoder stream failed for %s", clonedID.String())
			p.log.Error("failed to get stream from encoder", zap.Stringer("id", clonedID), zap.Error(err))
			writeErrors = append(writeErrors, err)
			goto seriesLoopEnd
		}

		if writeErr := writer.Write(clonedID, tagsData, streamSegment.Segment()); writeErr != nil {
			p.log.Error("failed to write series to rollup fileset", zap.Stringer("id", clonedID), zap.Error(writeErr))
			writeErrors = append(writeErrors, writeErr)
			clonedID.Finalize()
		} else {
			seriesCountWritten++
		}

	seriesLoopEnd:
		encoder.Close()
	}

	if err := writer.Close(); err != nil {
		writeErrors = append(writeErrors, fmt.Errorf("failed to close rollup writer: %w", err))
	}

	if len(writeErrors) > 0 {
		return fmt.Errorf("encountered %d errors during rollup processing for rule %s, first error: %w",
			len(writeErrors), rule.Resolution().String(), writeErrors[0])
	}

	if seriesCountWritten == 0 {
		p.log.Info("no series written to temporary rollup fileset", zap.String("tempDir", tempRollupDir))
		os.RemoveAll(tempRollupDir)
		return nil
	}

	p.log.Info("successfully wrote series to temporary rollup fileset",
		zap.Int("seriesWritten", seriesCountWritten),
		zap.String("tempDir", tempRollupDir))

	sourcePathForFileset := pfs.FilesetPathFromTime(
		tempFsOpts.FilePathPrefix(),
		ns.ID(),
		shardID,
		blockTime,
		tempFsOpts.InfoFilesPathFromTime(),
	)

	if _, err := os.Stat(sourcePathForFileset); os.IsNotExist(err) {
		return fmt.Errorf("source fileset path %s does not exist after write, cannot rename", sourcePathForFileset)
	}

	if err := os.Rename(sourcePathForFileset, rollupFilesetDir); err != nil {
		return fmt.Errorf("failed to rename temp rollup dir from %s to %s: %w", sourcePathForFileset, rollupFilesetDir, err)
	}
	p.log.Info("successfully moved rollup data to final location",
		zap.String("from", sourcePathForFileset),
		zap.String("to", rollupFilesetDir))

	os.RemoveAll(tempRollupDir)

	markerPath := p.markerFilePath(ns, shardID, blockTime, rule)
	if err := p.createRollupMarker(markerPath); err != nil {
		p.log.Error("failed to create rollup marker file after successful write and move",
			zap.String("namespace", nsIDStr),
			zap.Uint32("shard", shardID),
			zap.Time("blockTime", blockTime.ToTime()),
			zap.String("resolution", rule.Resolution().String()),
			zap.String("markerPath", markerPath),
			zap.Error(err),
		)
		return fmt.Errorf("rollup data processed and moved, but failed to create marker file %s: %w", markerPath, err)
	}
	p.log.Info("successfully created rollup marker file after processing rule and moving data", zap.String("markerPath", markerPath))

	return nil
}

func (p *rollupProcessor) aggregateSeries(
	id ident.ID,
	encodedData []byte,
	blockStart xtime.UnixNano,
	rollupResolution time.Duration,
	ns namespace.Metadata, // Added ns to potentially access schema
) ([]ts.Datapoint, error) {
	var aggregatedDps []ts.Datapoint

	if len(encodedData) == 0 {
		return aggregatedDps, nil
	}

	segment := ts.NewSegment(encodedData, nil, ts.FinalizeNone)
	// No defer segment.Finalize() needed due to FinalizeNone.

	iter := p.opts.multiReaderIterPool.Get()
	defer p.opts.multiReaderIterPool.Put(iter)

	// TODO: Replace nil with actual schemaDesc from namespace options
	// schemaDesc := ns.Options().SchemaHistory().Get(blockStart) // Or similar to get schema
	var schemaDesc namespace.SchemaDescr = nil

	segReader, err := encoding.NewSegmentReader(segment, schemaDesc)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment reader for series %s: %w", id.String(), err)
	}

	iter.Reset([]encoding.SegmentReader{segReader}, schemaDesc)

	dpsFromIterator := make([]ts.Datapoint, 0, segment.Len())

	for iter.Next() {
		dp, _, annotation := iter.Current()
		_ = annotation
		dpsFromIterator = append(dpsFromIterator, dp)
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterator error for series %s: %w", id.String(), err)
	}

	if len(dpsFromIterator) == 0 {
		return aggregatedDps, nil
	}

	// M3DB iterators generally return datapoints in chronological order.
	// If specific ordering is absolutely critical and not guaranteed by iterator, uncomment sort.
	// sort.Slice(dpsFromIterator, func(i, j int) bool {
	// 	return dpsFromIterator[i].TimestampNanos < dpsFromIterator[j].TimestampNanos
	// })

	if rollupResolution <= 0 {
		return nil, fmt.Errorf("rollup resolution must be positive, got %v", rollupResolution)
	}

	currentWindowStart := blockStart.Truncate(rollupResolution)
	currentWindowEnd := currentWindowStart.Add(rollupResolution)

	var lastDpInWindow ts.Datapoint
	foundDpInWindow := false

	for _, dp := range dpsFromIterator {
		if dp.TimestampNanos >= currentWindowEnd {
			if foundDpInWindow {
				aggregatedDps = append(aggregatedDps, lastDpInWindow)
			}
			currentWindowStart = dp.TimestampNanos.Truncate(rollupResolution)
			currentWindowEnd = currentWindowStart.Add(rollupResolution)
			foundDpInWindow = false
		}

		if dp.TimestampNanos >= currentWindowStart && dp.TimestampNanos < currentWindowEnd {
			lastDpInWindow = dp
			foundDpInWindow = true
		}
	}

	if foundDpInWindow {
		aggregatedDps = append(aggregatedDps, lastDpInWindow)
	}

	if len(aggregatedDps) > 0 {
			p.log.Debug("aggregated series",
			zap.Stringer("id", id),
			zap.Int("originalDps", len(dpsFromIterator)),
			zap.Int("aggregatedDps", len(aggregatedDps)),
			zap.Time("blockStart", blockStart.ToTime()),
			zap.Duration("resolution", rollupResolution))
	}

	return aggregatedDps, nil
}


func (p *rollupProcessor) rollupFilesetPath(
	ns namespace.Metadata,
	shardID uint32,
	blockTime xtime.UnixNano,
	rule retention.RollupRuleOptions,
) string {
	dataDir := p.opts.fsOpts.FilePathPrefix()
	blockDirName := fmt.Sprintf("%d", blockTime.UnixNano())
	resolutionStr := rule.Resolution().String()

	return filepath.Join(
		dataDir,
		ns.ID().String(),
		fmt.Sprintf("%d", shardID),
		blockDirName,
		fmt.Sprintf("%s_%s", rollupFileSetTypeStr, resolutionStr),
	)
}

func (p *rollupProcessor) createRollupMarker(filePath string) error {
	markerDir := filepath.Dir(filePath)
	if err := os.MkdirAll(markerDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory for marker file %s: %w", markerDir, err)
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create marker file %s: %w", filePath, err)
	}
	return file.Close()
}

func (p *rollupProcessor) NeedsRollup(
	ns namespace.Metadata,
	shardID uint32,
	blockTime xtime.UnixNano,
	rule retention.RollupRuleOptions,
) (bool, error) {
	// nowFn returns time.Time, convert to UnixNano for comparison if needed, or use time.Time directly.
	// For blockAge calculation, time.Time is fine.
	blockAge := p.nowFn().Sub(blockTime.ToTime()) // p.nowFn() returns time.Time

	if blockAge < rule.Age() {
		p.log.Debug("block not old enough for rollup rule",
			zap.String("namespace", ns.ID().String()),
			zap.Uint32("shard", shardID),
			zap.Time("blockTime", blockTime.ToTime()),
			zap.String("resolution", rule.Resolution().String()),
			zap.Duration("blockAge", blockAge),
			zap.Duration("ruleAge", rule.Age()),
		)
		return false, nil
	}

	markerPath := p.markerFilePath(ns, shardID, blockTime, rule)
	if _, err := os.Stat(markerPath); err == nil {
		p.log.Debug("rollup marker file exists, skipping",
			zap.String("namespace", ns.ID().String()),
			zap.Uint32("shard", shardID),
			zap.Time("blockTime", blockTime.ToTime()),
			zap.String("resolution", rule.Resolution().String()),
			zap.String("markerPath", markerPath),
		)
		return false, nil
	} else if !os.IsNotExist(err) {
		p.log.Error("error checking rollup marker file",
			zap.String("namespace", ns.ID().String()),
			zap.Uint32("shard", shardID),
			zap.Time("blockTime", blockTime.ToTime()),
			zap.String("resolution", rule.Resolution().String()),
			zap.String("markerPath", markerPath),
			zap.Error(err),
		)
		return false, err
	}

	p.log.Debug("rollup needed",
		zap.String("namespace", ns.ID().String()),
		zap.Uint32("shard", shardID),
		zap.Time("blockTime", blockTime.ToTime()),
		zap.String("resolution", rule.Resolution().String()),
	)
	return true, nil
}

func (p *rollupProcessor) markerFilePath(
	ns namespace.Metadata,
	shardID uint32,
	blockTime xtime.UnixNano,
	rule retention.RollupRuleOptions,
) string {
	fsOpts := p.opts.fsOpts
	dataDir := fsOpts.FilePathPrefix()
	blockDirName := fmt.Sprintf("%d", blockTime.UnixNano())
	resolutionStr := rule.Resolution().String()

	path := filepath.Join(
		dataDir,
		ns.ID().String(),
		fmt.Sprintf("%d", shardID),
		blockDirName,
		fmt.Sprintf("rollup_%s.%s", resolutionStr, rollupMarkerFileSuffix),
	)
	return path
}

// Ensure all necessary imports are included.
// This initial setup focuses on the structure and basic logic.

// Removed placeholder BlockRetriever interface as it's not used.
// Removed placeholder FilesystemOptions interface as it's replaced by pfs.Options.
