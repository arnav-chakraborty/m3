package rollup

import (
	"fmt"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/uber-go/tally"
)

const (
	// Metrics tags.
	namespaceTag    = "namespace"
	shardTag        = "shard"
	resolutionTag   = "resolution"
	errorTypeTag    = "error_type"
)

// Metrics defines the metrics for the rollup process.
type Metrics interface {
	// Scope returns the underlying scope.
	Scope() tally.Scope

	// ReportAttempt reports a rollup attempt.
	ReportAttempt()

	// ReportSuccess reports a successful rollup.
	ReportSuccess(duration time.Duration)

	// ReportFailure reports a failed rollup.
	ReportFailure(errType string)

	// ReportSeriesRead reports a series read from the original fileset.
	ReportSeriesRead(count int64)

	// ReportSeriesWritten reports a series written to the rollup fileset.
	ReportSeriesWritten(count int64)

	// ReportDataReadBytes reports bytes read from the original fileset.
	ReportDataReadBytes(count int64)

	// ReportDataWrittenBytes reports bytes written to the rollup fileset.
	ReportDataWrittenBytes(count int64)

	// IncActive increments the active rollups gauge.
	IncActive()

	// DecActive decrements the active rollups gauge.
	DecActive()
}

type rollupMetrics struct {
	scope            tally.Scope
	rollupAttempts   tally.Counter
	rollupSuccess    tally.Counter
	rollupFailures   tally.Counter // Untagged total failures
	rollupDuration   tally.Timer
	seriesRead       tally.Counter
	seriesWritten    tally.Counter
	dataReadBytes    tally.Counter
	dataWrittenBytes tally.Counter
	activeRollups    tally.Gauge

	// Specific failure counters
	readerOpenFailures      tally.Counter
	writerOpenFailures      tally.Counter
	seriesReadFailures      tally.Counter
	seriesWriteFailures     tally.Counter
	writerFinalizeFailures  tally.Counter
	readerFinalizeFailures  tally.Counter
	prepareFailures         tally.Counter
}

// NewMetrics creates new rollup metrics.
// The scope passed here should ideally be the base scope for rollup operations,
// further tagging can happen when a specific RollupFileSet operation starts.
func NewMetrics(scope tally.Scope) Metrics {
	failuresScope := scope.SubScope("failures")
	return &rollupMetrics{
		scope:            scope,
		rollupAttempts:   scope.Counter("attempts"),
		rollupSuccess:    scope.Counter("success"),
		rollupFailures:   scope.Counter("failures_total"), // Total failures
		rollupDuration:   scope.Timer("duration"),
		seriesRead:       scope.Counter("series-read"),
		seriesWritten:    scope.Counter("series-written"),
		dataReadBytes:    scope.Counter("data-read-bytes"),
		dataWrittenBytes: scope.Counter("data-written-bytes"),
		activeRollups:    scope.Gauge("active-rollups"),

		readerOpenFailures:     failuresScope.Tagged(map[string]string{errorTypeTag: "reader_open"}).Counter("count"),
		writerOpenFailures:     failuresScope.Tagged(map[string]string{errorTypeTag: "writer_open"}).Counter("count"),
		seriesReadFailures:     failuresScope.Tagged(map[string]string{errorTypeTag: "series_read"}).Counter("count"),
		seriesWriteFailures:    failuresScope.Tagged(map[string]string{errorTypeTag: "series_write"}).Counter("count"),
		writerFinalizeFailures: failuresScope.Tagged(map[string]string{errorTypeTag: "writer_finalize"}).Counter("count"),
		readerFinalizeFailures: failuresScope.Tagged(map[string]string{errorTypeTag: "reader_finalize"}).Counter("count"),
		prepareFailures:        failuresScope.Tagged(map[string]string{errorTypeTag: "prepare"}).Counter("count"),
	}
}

func (m *rollupMetrics) Scope() tally.Scope {
	return m.scope
}

func (m *rollupMetrics) ReportAttempt() {
	m.rollupAttempts.Inc(1)
}

func (m *rollupMetrics) ReportSuccess(duration time.Duration) {
	m.rollupSuccess.Inc(1)
	m.rollupDuration.Record(duration)
}

func (m *rollupMetrics) ReportFailure(errType string) {
	m.rollupFailures.Inc(1) // Increment total failures
	// Increment specific failure counter based on errType
	switch errType {
	case "reader_open":
		m.readerOpenFailures.Inc(1)
	case "writer_open":
		m.writerOpenFailures.Inc(1)
	case "series_read":
		m.seriesReadFailures.Inc(1)
	case "series_write":
		m.seriesWriteFailures.Inc(1)
	case "writer_finalize":
		m.writerFinalizeFailures.Inc(1)
	case "reader_finalize":
		m.readerFinalizeFailures.Inc(1)
	case "prepare":
		m.prepareFailures.Inc(1)
	default:
		// For unknown error types, could have a generic tagged failure or just rely on total
		m.scope.Tagged(map[string]string{errorTypeTag: "unknown"}).Counter("failures_by_type").Inc(1)
	}
}

func (m *rollupMetrics) ReportSeriesRead(count int64) {
	m.seriesRead.Inc(count)
}

func (m *rollupMetrics) ReportSeriesWritten(count int64) {
	m.seriesWritten.Inc(count)
}

func (m *rollupMetrics) ReportDataReadBytes(count int64) {
	m.dataReadBytes.Inc(count)
}

func (m *rollupMetrics) ReportDataWrittenBytes(count int64) {
	m.dataWrittenBytes.Inc(count)
}

func (m *rollupMetrics) IncActive() {
	m.activeRollups.Inc(1)
}

func (m *rollupMetrics) DecActive() {
	m.activeRollups.Dec(1)
}

// TagsForFileSetInfo creates a standard set of tags for metrics related to a fileset.
func TagsForFileSetInfo(originalFileSet persist.FileSetVolumeInfo, ro Options) map[string]string {
	return map[string]string{
		namespaceTag:  originalFileSet.Namespace.String(),
		shardTag:      fmt.Sprintf("%d", originalFileSet.Shard),
		resolutionTag: ro.Resolution().String(), // Assuming rollup.Options has Resolution()
	}
}
