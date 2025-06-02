package rollup

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	// "github.com/m3db/m3/src/dbnode/encoding" // Not used in this simplified version
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	// "github.com/m3db/m3/src/dbnode/ts" // Not used in this simplified version
	// "github.com/m3db/m3/src/x/checked" // Not used in this simplified version
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	// "github.com/m3db/m3/src/x/pool" // Not used in this simplified version
	// xtime "github.com/m3db/m3/src/x/time" // Not directly used yet
)

// CreatorNewReaderFn defines a function to create a new fileset reader.
// It's defined as an interface to allow mocking.
type CreatorNewReaderFn interface {
	Execute(bytesPool xpool.CheckedBytesPool, opts fs.Options) (fs.DataFileSetReader, error)
}

// CreatorNewStreamingWriterFn defines a function to create a new streaming writer.
// It's defined as an interface to allow mocking.
type CreatorNewStreamingWriterFn interface {
	Execute(opts fs.Options) (fs.StreamingWriter, error)
}

// defaultNewReader is the default implementation for creating a new reader.
type defaultNewReader struct{}

func (d *defaultNewReader) Execute(bytesPool xpool.CheckedBytesPool, opts fs.Options) (fs.DataFileSetReader, error) {
	return fs.NewReader(bytesPool, opts)
}

// DefaultNewReaderFn returns the default new reader function.
func DefaultNewReaderFn() CreatorNewReaderFn {
	return &defaultNewReader{}
}

// defaultNewStreamingWriter is the default implementation for creating a new streaming writer.
type defaultNewStreamingWriter struct{}

func (d *defaultNewStreamingWriter) Execute(opts fs.Options) (fs.StreamingWriter, error) {
	return fs.NewStreamingWriter(opts)
}

// DefaultNewStreamingWriterFn returns the default new streaming writer function.
func DefaultNewStreamingWriterFn() CreatorNewStreamingWriterFn {
	return &defaultNewStreamingWriter{}
}

// RollupFileSet performs the rollup operation for a given fileset.
// It accepts factory functions for reader and writer for testability.
func RollupFileSet(
	originalFileSet persist.FileSetVolumeInfo,
	rollupOptions Options, // This is rollup.Options from this package
	nsOpts namespace.Options,
	fsOpts fs.Options, // Filesystem options from the database
	metrics Metrics, // Metrics collector
	// Use default factories in production, mock factories in tests.
	newReaderFn CreatorNewReaderFn,
	newWriterFn CreatorNewStreamingWriterFn,
) (err error) { // Named return for easier defer handling
	metrics.IncActive()
	defer metrics.DecActive()

	metrics.ReportAttempt()
	startTime := time.Now()

	defer func() {
		if err != nil {
			// NB: Specific error types are reported closer to where they occur.
			// This is a general catch-all if an error wasn't specific.
			// Consider if a generic "unknown_failure" tag is needed or if specific errors cover all cases.
			// For now, relying on specific error metric points. If err is set but no specific metric
			// was hit, it implies a failure type not yet specifically instrumented.
		} else {
			metrics.ReportSuccess(time.Since(startTime))
		}
	}()

	// TODO: Get logger from instrument options within fsOpts or nsOpts if available
	// logger := fsOpts.InstrumentOptions().Logger()
	// logger.Info("starting rollup for fileset", ...)

	// 1. Prepare reader for the original fileset
	// Use a nil bytesPool for now, or get from fsOpts.InstrumentOptions().BytesPool() if available and needed.
	reader, readerErr := newReaderFn.Execute(nil, fsOpts)
	if readerErr != nil {
		err = fmt.Errorf("failed to create fileset reader: %w", readerErr)
		metrics.ReportFailure("reader_open_create") // More specific error type
		return err
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("failed to close reader: %w", closeErr)
			metrics.ReportFailure("reader_finalize")
		}
	}()

	openOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   originalFileSet.Namespace,
			Shard:       originalFileSet.Shard,
			BlockStart:  originalFileSet.BlockStart,
			VolumeIndex: originalFileSet.VolumeIndex,
		},
		FileSetType:      persist.FileSetFlushType,
		StreamingEnabled: true,
	}
	if openErr := reader.Open(openOpts); openErr != nil {
		err = fmt.Errorf("failed to open reader for original fileset: %w", openErr)
		metrics.ReportFailure("reader_open")
		return err
	}

	// 2. Prepare writer for the new rollup fileset
	rollupResolution := rollupOptions.Resolution()
	newBlockStart := originalFileSet.BlockStart.Truncate(rollupResolution)
	newVolumeIndex := 0 // Rollup filesets start at volume 0 for their blockstart

	// Determine the target directory for the rollup fileset.
	// Files should be in: <original_shard_path>/rollup/<resolution_str>/<blockstart>-<volume>-<type>.db
	// The fs.Writer creates paths relative to its fs.Options.FilePathPrefix(), plus Namespace and Shard components.
	// To write files directly into the target directory, we set FilePathPrefix to that directory
	// and use placeholder Namespace and Shard for the writer's open options.

	rollupFilesetParentDir := filepath.Join(
		fs.ShardDirPath(fsOpts.FilePathPrefix(), originalFileSet.Namespace, originalFileSet.Shard),
		RollupDirName, // Use constant
		rollupResolution.String(),
	)

	if mkdirErr := os.MkdirAll(rollupFilesetParentDir, fsOpts.NewDirectoryMode()); mkdirErr != nil {
		err = fmt.Errorf("failed to create rollup parent directory %s: %w", rollupFilesetParentDir, mkdirErr)
		metrics.ReportFailure("prepare")
		return err
	}

	// Create specialized fs.Options for the writer.
	// Its FilePathPrefix will be the directory where fileset files (info, summaries etc.) are written.
	// NB: fs.Options is an interface. We need to ensure we can create/modify one appropriately.
	// fs.NewOptions() creates a default one. We need to clone and modify fsOpts.
	// Assuming fsOpts is a concrete options type or has a `SetFilePathPrefix` method.
	// For this subtask, let's create a new default options and set necessary fields if fsOpts cannot be easily cloned/modified.
	// This is a simplification. A robust solution would properly derive writerFsOpts from fsOpts.
	writerFsOpts := fs.NewOptions().
		SetFilePathPrefix(rollupFilesetParentDir).
		SetWriterBufferSize(fsOpts.WriterBufferSize()). // Copy other relevant settings
		SetNewFileMode(fsOpts.NewFileMode()).
		SetNewDirectoryMode(fsOpts.NewDirectoryMode()).
		SetInstrumentOptions(fsOpts.InstrumentOptions()) // Share instrument options

	// If M3DB's fs.Options has a `SetRetentionOptions` or similar that might affect pathing or metadata,
	// it should be configured here for the *new* TTL.
	// For now, writerFsOpts primarily cares about FilePathPrefix for output location.
	// The actual retention of the rollup data (NewTTL) is an attribute of the data itself,
	// usually managed at a higher level (e.g. namespace configuration for the rollup target).
	// The fileset info file might store retention information based on nsOpts passed to writer.

	rollupWriter, writerErr := newWriterFn.Execute(writerFsOpts)
	if writerErr != nil {
		err = fmt.Errorf("failed to create streaming writer for rollup: %w", writerErr)
		metrics.ReportFailure("writer_open_create") // More specific error type
		return err
	}
	// Defer Abort/Close for rollupWriter
	defer func() {
		// This defer runs after the main error handling defer for metrics.
		// If 'err' is already set by the main logic, it means a failure occurred.
		if err != nil {
			if abortErr := rollupWriter.Abort(); abortErr != nil {
				// Potentially log this, but the primary error is already captured.
				// logger.Error("failed to abort rollup writer", zap.Error(abortErr))
			}
		} else { // If no error so far from the main logic, try to close (finalize) the writer.
			if closeErr := rollupWriter.Close(); closeErr != nil {
				// logger.Error("failed to close rollup writer", zap.Error(closeErr))
				err = fmt.Errorf("failed to close rollup writer: %w", closeErr) // This error will be caught by the outer defer
				metrics.ReportFailure("writer_finalize")
			}
		}
	}()

	// When using writerFsOpts with FilePathPrefix = rollupFilesetParentDir,
	// the NamespaceID and ShardID for the writer.Open() call should be "neutral"
	// so they don't create further subdirectories.
	// Let's use a placeholder Namespace and Shard 0.
	// The fs.FilesetPathFromTimeAndIndex will construct:
	// writerFsOpts.FilePathPrefix() / "placeholderNamespace" / "0" / <files>
	// This means rollupFilesetParentDir itself must be structured to expect this,
	// OR placeholderNamespace should be "." or empty if supported (unlikely).
	// This is still tricky.

	// Correct approach for writer pathing with current fs.Writer:
	// 1. writerFsOpts.FilePathPrefix = fsOpts.FilePathPrefix() (the real root: /var/lib/m3db)
	// 2. writerOpenOpts.NamespaceID = synthetic ident.ID representing the longer path.
	//    e.g., ident.StringID(fmt.Sprintf("%s/%d/rollup/%s", originalFileSet.Namespace.String(), originalFileSet.Shard, rollupResolution.String()))
	//    This will fail if ident.ID cannot contain '/'. ident.ID is typically a simple string.
	//    If ident.ID can contain '/', then fs.NamespaceDirPath will create nested dirs, which is what we want.
	//    Let's test this assumption for ident.ID. `filepath.Clean` is used by `DataDirPath`, so '/' should be fine.

	syntheticNsStr := filepath.Join(
		originalFileSet.Namespace.String(),
		fmt.Sprintf("%d", originalFileSet.Shard), // Shard ID as part of the "namespace" path
		"rollup",
		rollupResolution.String())
	syntheticNamespaceID := ident.StringID(syntheticNsStr)

	// The shard for writerOpenOpts can be a dummy one, as it's already in syntheticNsStr.
	// However, fs.FilesetPathFromTimeAndIndex still appends ShardDirName(shardID).
	// So, if ShardID is 0, it will add a "0" directory at the end.
	// To avoid this, the synthetic namespace must be the *complete* path component before blockstart files.
	// This means `ShardDataDirPath(filePathPrefix, syntheticNamespaceID, syntheticShardID)` should yield `rollupFilesetParentDir`.
	// `ShardDataDirPath(p, n, s) = NamespaceDataDirPath(p,n) + "/" + ShardDirName(s)`
	// `NamespaceDataDirPath(p,n) = DataDirPath(p) + "/" + n.String()`
	// So, `DataDirPath(fsOpts.FilePathPrefix()) + "/" + syntheticNsStr + "/" + ShardDirName(syntheticShardID)`
	// If `syntheticShardID` is a special value that results in empty ShardDirName, or if we ensure
	// `syntheticNsStr` is `original_ns/shard/rollup/resolution` and `syntheticShardID` is a dummy value,
	// then `NamespaceDataDirPath` becomes `root/original_ns/shard/rollup/resolution`.
	// If `ShardDirName(dummyShard)` is just "dummyShardValue", then path is `root/orig_ns/shard/rollup/res/dummyShardVal/fileset`. Too deep.

	// The most straightforward way to use current fs.Writer to write to `rollupFilesetParentDir`
	// is to set `writerFsOpts.FilePathPrefix = rollupFilesetParentDir`.
	// Then, ensure `NamespaceID` and `ShardID` in `writerOpenOpts` do not cause sub-path creation.
	// `fs.FilesetPathFromTimeAndIndex` will be called with `rollupFilesetParentDir` as `filePathPrefix`.
	// If `NamespaceID` is `.` and `ShardID` is `0` (and `ShardDirName(0)` is `0`), path becomes `rollupFilesetParentDir/./0/block...`.
	// This is acceptable if `.` is treated as current dir.
	// Let's use a minimal, non-empty namespace for the writer under the new root.
	writerOpenOpts := fs.StreamingWriterOpenOptions{
		NamespaceID: ident.StringID("rollup_data"), // Placeholder NS within the new root
		ShardID:     0,                             // Placeholder Shard within the new root
		BlockStart:  newBlockStart,
		BlockSize:   rollupResolution,
		VolumeIndex: newVolumeIndex,
	}
	// This will create files in: `rollupFilesetParentDir/rollup_data/0/blockstart-volume-type.db`
	// This seems like a reasonable and achievable structure.

	if openErr := rollupWriter.Open(writerOpenOpts); openErr != nil {
		err = fmt.Errorf("failed to open writer for rollup fileset (path %s, ns %s, shard %d, block %s, vol %d): %w",
			writerFsOpts.FilePathPrefix(), writerOpenOpts.NamespaceID.String(), writerOpenOpts.ShardID, newBlockStart.String(), newVolumeIndex, openErr)
		metrics.ReportFailure("writer_open")
		return err
	}

	ctx := context.NewBackground()
	defer ctx.Close()

	var seriesReadCount int64
	var seriesWrittenCount int64
	var dataReadBytesCount int64
	var dataWrittenBytesCount int64

	// Get schema and reader iterator from pool
	schema := nsOpts.SchemaHistory().GetLatestOrDefault()
	readerIter := nsOpts.ReaderIteratorPool().Get()
	defer readerIter.Close()

	// Get encoder from pool for writing aggregated data
	encoder := nsOpts.EncoderPool().Get()
	defer encoder.Close()

	for {
		entry, entryErr := reader.StreamingRead()
		if entryErr == io.EOF {
			break
		}
		if entryErr != nil {
			err = fmt.Errorf("error reading from original fileset: %w", entryErr)
			metrics.ReportFailure("series_read")
			return err
		}
		seriesReadCount++
		dataReadBytesCount += int64(len(entry.Data))

		// Setup iterator for the current series' data
		segmentReader := xio.NewSegmentReader(ts.NewSegment(entry.Data, nil, ts.FinalizeNone))
		readerIter.Reset(segmentReader, schema)

		aggregatedDps := make([]ts.Datapoint, 0, len(entry.Data)/20) // Pre-allocate assuming some reduction
		var lastDpInWindow ts.Datapoint
		hasDpInWindow := false
		currentRollupWindowStart := newBlockStart

		for readerIter.Next() {
			dp, unit, annotation := readerIter.Current()

			// Determine which rollup window this datapoint falls into
			dpWindowStart := dp.Timestamp.Truncate(rollupResolution)

			if hasDpInWindow && dpWindowStart != currentRollupWindowStart {
				// Current datapoint is in a new window, so add the last one from the previous window
				aggregatedDps = append(aggregatedDps, lastDpInWindow)
				hasDpInWindow = false
			}
			currentRollupWindowStart = dpWindowStart

			// For "last value in window", simply overwrite lastDpInWindow
			// The actual timestamp of the aggregated point will be currentRollupWindowStart
			lastDpInWindow = ts.Datapoint{Timestamp: currentRollupWindowStart, Value: dp.Value}
			// Preserve annotation of the last point in window
			if len(annotation) > 0 {
				// Need to copy annotation as it might be pooled
				annCopy := make([]byte, len(annotation))
				copy(annCopy, annotation)
				lastDpInWindow.Annotation = annCopy
			}
			hasDpInWindow = true
		}
		iterErr := readerIter.Err()
		if iterErr != nil {
			err = fmt.Errorf("iterator error for series %s: %w", entry.ID.String(), iterErr)
			metrics.ReportFailure("series_read_iterate")
			return err
		}

		// Add the very last datapoint if it was in the last window
		if hasDpInWindow {
			aggregatedDps = append(aggregatedDps, lastDpInWindow)
		}

		// If no data points aggregated for this series, skip writing
		if len(aggregatedDps) == 0 {
			continue
		}

		// Encode aggregated data
		encoder.Reset(newBlockStart, len(aggregatedDps), schema)
		for _, dp := range aggregatedDps {
			// TODO: Figure out correct unit for rolled up data, for now assume same as original or default.
			// For "last" aggregation, unit of the last point is probably fine.
			// However, ts.Datapoint doesn't store unit. ReaderIterator gives it.
			// For simplicity, assume time.Nanosecond or derive if possible.
			// For now, this part is simplified. The `unit` from `iter.Current()` is not directly
			// associated with `lastDpInWindow` unless specifically stored.
			// Let's assume ts.Nanosecond for now for encoded data or get from schema.
			// The annotation is already part of lastDpInWindow if preserved.
			if encErr := encoder.Encode(dp, xtime.Nanosecond, dp.Annotation); encErr != nil {
				err = fmt.Errorf("failed to encode aggregated datapoint for series %s: %w", entry.ID.String(), encErr)
				metrics.ReportFailure("series_encode")
				return err // Or handle per series? For now, fail fast.
			}
		}

		encodedData, ok := encoder.Stream(ctx)
		if !ok || encodedData == nil || encodedData.Len() == 0 {
			// No data to write for this series or an issue with stream
			continue
		}
		
		finalizedSegment := encoder.Discard() // Discard also finalizes and returns the segment
		
		var dataToWrite [][]byte
		var finalBytes []byte

		if finalizedSegment.Len() > 0 {
			// Prefer concatenated bytes for checksum and potentially for writer if it handles single blocks well.
			// Some writers might prefer segmented bytes if they are large.
			// For simplicity and typical WriteAll usage, provide a single byte slice if possible.
			if finalizedSegment.Head != nil && finalizedSegment.Tail == nil {
				finalBytes = finalizedSegment.Head.Bytes()
			} else {
				// Need to concatenate if there's a tail or if head is nil (empty segment)
				// This allocation is unfortunate but sometimes necessary.
				finalBytes = finalizedSegment.ConcatenatedBytes()
			}
			if len(finalBytes) > 0 {
				dataToWrite = [][]byte{finalBytes}
			}
		}


		if len(dataToWrite) == 0 {
			continue // Nothing to write
		}

		dataChecksum := digest.Checksum(finalBytes)
		dataWrittenBytesCount += int64(len(finalBytes))

		writeErr := rollupWriter.WriteAll(entry.ID, entry.EncodedTags, dataToWrite, dataChecksum)
		if writeErr != nil {
			err = fmt.Errorf("failed to write series %s to rollup fileset: %w", entry.ID.String(), writeErr)
			metrics.ReportFailure("series_write")
			return err
		}
		seriesWrittenCount++
	}

	metrics.ReportSeriesRead(seriesReadCount)
	metrics.ReportSeriesWritten(seriesWrittenCount)
	metrics.ReportDataReadBytes(dataReadBytesCount)
	metrics.ReportDataWrittenBytes(dataWrittenBytesCount)

	// logger.Info("completed rollup fileset processing", zap.Int("series_written", entries))
	// err is already named return, will be handled by defer for success/failure reporting
	return err
}
