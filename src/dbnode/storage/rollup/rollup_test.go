package rollup

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/encoding"
	tsenc "github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/instrument"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

// Mock fs.Reader
type mockFsReader struct {
	ctrl            *gomock.Controller
	openFn          func(opts fs.DataReaderOpenOptions) error
	streamingReadFn func() (fs.FileSetDataEntry, error)
	closeFn         func() error
	entriesFn       func() int // To simulate multiple calls to StreamingRead
	err             error      // To simulate errors during operations
}

func newMockFsReader(ctrl *gomock.Controller) *mockFsReader {
	return &mockFsReader{ctrl: ctrl}
}
func (m *mockFsReader) Open(opts fs.DataReaderOpenOptions) error {
	if m.openFn != nil {
		return m.openFn(opts)
	}
	return m.err
}
func (m *mockFsReader) StreamingRead() (fs.FileSetDataEntry, error) {
	if m.streamingReadFn != nil {
		return m.streamingReadFn()
	}
	return fs.FileSetDataEntry{}, m.err
}
func (m *mockFsReader) Entries() int {
	if m.entriesFn != nil {
		return m.entriesFn()
	}
	return 0
}
func (m *mockFsReader) EntriesRead() int { return 0 }
func (m *mockFsReader) Range() xtime.Range { return xtime.Range{} }
func (m *mockFsReader) Read() (id ident.ID, tags ident.TagIterator, data []byte, checksum uint32, err error) {
	return nil, nil, nil, 0, fmt.Errorf("not implemented in mock")
}
func (m *mockFsReader) Validate() error { return nil }
func (m *mockFsReader) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return m.err
}

// Mock fs.StreamingWriter
type mockFsStreamingWriter struct {
	ctrl    *gomock.Controller
	openFn  func(opts fs.StreamingWriterOpenOptions) error
	writeFn func(id ident.ID, tags ident.TagIterator, data [][]byte, checksum uint32) error
	closeFn func() error
	abortFn func() error
	err     error
}

func newMockFsStreamingWriter(ctrl *gomock.Controller) *mockFsStreamingWriter {
	return &mockFsStreamingWriter{ctrl: ctrl}
}
func (m *mockFsStreamingWriter) Open(opts fs.StreamingWriterOpenOptions) error {
	if m.openFn != nil {
		return m.openFn(opts)
	}
	return m.err
}
func (m *mockFsStreamingWriter) WriteAll(id ident.ID, tags ident.TagIterator, data [][]byte, checksum uint32) error {
	if m.writeFn != nil {
		return m.writeFn(id, tags, data, checksum)
	}
	return m.err
}
func (m *mockFsStreamingWriter) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return m.err
}
func (m *mockFsStreamingWriter) Abort() error {
	if m.abortFn != nil {
		return m.abortFn()
	}
	return nil // Abort usually doesn't return an error unless it's catastrophic
}

//go:generate mockgen -destination=./metrics_mock_test.go -package=rollup github.com/m3db/m3/src/dbnode/storage/rollup Metrics
//go:generate mockgen -destination=./creator_mock_test.go -package=rollup github.com/m3db/m3/src/dbnode/storage/rollup CreatorNewReaderFn,CreatorNewStreamingWriterFn

func TestRollupFileSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockNsOpts := namespace.NewMockOptions(ctrl)
	mockFsOpts := fs.NewMockOptions(ctrl)
	mockRollupMetrics := NewMockMetrics(ctrl)
	mockNewReaderFactory := NewMockCreatorNewReaderFn(ctrl)
	mockNewWriterFactory := NewMockCreatorNewStreamingWriterFn(ctrl)

	// Default behaviors for mocks
	mockNsOpts.EXPECT().RetentionOptions().Return(namespace.NewRetentionOptions()).AnyTimes()
	mockFsOpts.EXPECT().FilePathPrefix().Return("test_prefix").AnyTimes()
	mockFsOpts.EXPECT().NewDirectoryMode().Return(os.ModePerm).AnyTimes()
	mockFsOpts.EXPECT().WriterBufferSize().Return(0).AnyTimes()
	mockFsOpts.EXPECT().NewFileMode().Return(os.ModePerm).AnyTimes()
	mockFsOpts.EXPECT().InstrumentOptions().Return(instrument.NewOptions()).AnyTimes()

	// Mock schema and pools
	mockSchema := namespace.NewMockSchemaDescr(ctrl)
	mockSchema.EXPECT().EncoderOptions().Return(nil).AnyTimes()

	mockSchemaHistory := namespace.NewMockSchemaHistory(ctrl)
	mockSchemaHistory.EXPECT().GetLatestOrDefault().Return(mockSchema).AnyTimes()
	mockNsOpts.EXPECT().SchemaHistory().Return(mockSchemaHistory).AnyTimes()

	mockReaderIter := encoding.NewMockReaderIterator(ctrl)
	mockReaderIterPool := encoding.NewMockReaderIteratorPool(ctrl)
	mockReaderIterPool.EXPECT().Get().Return(mockReaderIter).AnyTimes()
	mockReaderIter.EXPECT().Close().AnyTimes() // Ensure it's always closed
	mockNsOpts.EXPECT().ReaderIteratorPool().Return(mockReaderIterPool).AnyTimes()

	mockEncoder := encoding.NewMockEncoder(ctrl)
	mockEncoderPool := encoding.NewMockEncoderPool(ctrl)
	mockEncoderPool.EXPECT().Get().Return(mockEncoder).AnyTimes()
	mockEncoder.EXPECT().Close().AnyTimes() // Ensure it's always closed
	mockNsOpts.EXPECT().EncoderPool().Return(mockEncoderPool).AnyTimes()

	defaultOriginalFileSet := persist.FileSetVolumeInfo{
		Namespace:   ident.StringID("testns"),
		Shard:       0,
		BlockStart:  xtime.FromSeconds(0),
		VolumeIndex: 0,
	}
	defaultRollupResolution := time.Hour
	defaultRollupOptions := NewOptions().SetResolution(defaultRollupResolution).SetNewTTL(24 * time.Hour)
	expectedNewBlockStart := defaultOriginalFileSet.BlockStart.Truncate(defaultRollupResolution)

	testCases := []struct {
		name            string
		setupReader     func(*mockFsReader, []ts.Datapoint)
		setupWriter     func(*mockFsStreamingWriter, []ts.Datapoint)
		setupMetrics    func(*MockMetrics)
		setupEncoder    func(*encoding.MockEncoder, []ts.Datapoint)
		setupReaderIter func(*encoding.MockReaderIterator, []ts.Datapoint)
		testData        []ts.Datapoint
		expectedAggData []ts.Datapoint
		expectedErr     string
		originalFileSet persist.FileSetVolumeInfo
		rollupOptions   Options
		nsOpts          namespace.Options
		fsOpts          fs.Options
	}{
		{
			name: "basic success case with aggregation",
			testData: []ts.Datapoint{
				{Timestamp: xtime.FromSeconds(0), Value: 1.0},
				{Timestamp: xtime.FromSeconds(1800), Value: 2.0}, 
				{Timestamp: xtime.FromSeconds(3600 + 600), Value: 3.0},
				{Timestamp: xtime.FromSeconds(3600 + 1200), Value: 4.0},
				{Timestamp: xtime.FromSeconds(7200 + 300), Value: 5.0},
			},
			expectedAggData: []ts.Datapoint{
				{Timestamp: expectedNewBlockStart, Value: 2.0},
				{Timestamp: expectedNewBlockStart.Add(1 * time.Hour), Value: 4.0},
				{Timestamp: expectedNewBlockStart.Add(2 * time.Hour), Value: 5.0},
			},
			setupReader: func(r *mockFsReader, data []ts.Datapoint) {
				r.openFn = func(o fs.DataReaderOpenOptions) error { return nil }
				encoder := tsenc.NewEncoder(expectedNewBlockStart, nil, tsenc.DefaultEncodingOptions, mockSchema)
				for _, dp := range data {
					encoder.Encode(dp, xtime.Second, nil)
				}
				seg := encoder.Discard()
				var rawData []byte
				if seg.Head != nil { rawData = append(rawData, seg.Head.Bytes()...) }
				if seg.Tail != nil { rawData = append(rawData, seg.Tail.Bytes()...) }

				readCallCount := 0
				r.streamingReadFn = func() (fs.FileSetDataEntry, error) {
					if readCallCount == 0 {
						readCallCount++
						return fs.FileSetDataEntry{ID: ident.StringID("foo"), Data: rawData}, nil
					}
					return fs.FileSetDataEntry{}, io.EOF
				}
				r.closeFn = func() error { return nil }
			},
			setupReaderIter: func(iter *encoding.MockReaderIterator, data []ts.Datapoint) {
				dpIdx := -1
				iter.EXPECT().Next().DoAndReturn(func() bool {
					dpIdx++
					return dpIdx < len(data)
				}).AnyTimes()
				iter.EXPECT().Current().DoAndReturn(func() (ts.Datapoint, xtime.Unit, ts.Annotation) {
					return data[dpIdx], xtime.Second, nil
				}).AnyTimes()
				iter.EXPECT().Reset(gomock.Any(), gomock.Any()).AnyTimes()
				iter.EXPECT().Err().Return(nil).AnyTimes()
			},
			setupWriter: func(w *mockFsStreamingWriter, aggData []ts.Datapoint) {
				w.openFn = func(o fs.StreamingWriterOpenOptions) error {
					require.Equal(t, expectedNewBlockStart, o.BlockStart)
					require.Equal(t, defaultRollupResolution, o.BlockSize)
					return nil
				}
				w.writeFn = func(id ident.ID, tags ident.TagIterator, data [][]byte, checksum uint32) error {
					require.Equal(t, "foo", id.String())
					expectedEncoder := tsenc.NewEncoder(expectedNewBlockStart, nil, tsenc.DefaultEncodingOptions, mockSchema)
					for _, dp := range aggData {
						expectedEncoder.Encode(dp, xtime.Nanosecond, dp.Annotation)
					}
					expectedSegment := expectedEncoder.Discard()
					var expectedBytes []byte
					if expectedSegment.Head != nil { expectedBytes = append(expectedBytes, expectedSegment.Head.Bytes()...) }
					if expectedSegment.Tail != nil { expectedBytes = append(expectedBytes, expectedSegment.Tail.Bytes()...) }

					require.Equal(t, [][]byte{expectedBytes}, data)
					require.Equal(t, digest.Checksum(expectedBytes), checksum)
					return nil
				}
				w.closeFn = func() error { return nil }
			},
			setupEncoder: func(enc *encoding.MockEncoder, aggData []ts.Datapoint) {
				enc.EXPECT().Reset(expectedNewBlockStart, len(aggData), mockSchema).Times(1)
				for _, dp := range aggData {
					enc.EXPECT().Encode(dp, xtime.Nanosecond, dp.Annotation).Return(nil).Times(1)
				}
				enc.EXPECT().Discard().DoAndReturn(func() ts.Segment {
					tempEncoder := tsenc.NewEncoder(expectedNewBlockStart, nil, tsenc.DefaultEncodingOptions, mockSchema)
					for _, dp := range aggData {
						tempEncoder.Encode(dp, xtime.Nanosecond, dp.Annotation)
					}
					return tempEncoder.Discard()
				}).Times(1)
			},
			setupMetrics: func(m *MockMetrics) {
				m.EXPECT().IncActive(); m.EXPECT().DecActive(); m.EXPECT().ReportAttempt()
				m.EXPECT().ReportSeriesRead(int64(1)); m.EXPECT().ReportSeriesWritten(int64(1))
				m.EXPECT().ReportDataReadBytes(gomock.Any()); m.EXPECT().ReportDataWrittenBytes(gomock.Any())
				m.EXPECT().ReportSuccess(gomock.Any())
			},
			originalFileSet: defaultOriginalFileSet, rollupOptions: defaultRollupOptions, nsOpts: mockNsOpts, fsOpts: mockFsOpts,
		},
		{
			name: "reader create failure",
			setupMetrics: func(m *MockMetrics) {
				m.EXPECT().IncActive(); m.EXPECT().DecActive(); m.EXPECT().ReportAttempt(); m.EXPECT().ReportFailure("reader_open_create")
			},
			expectedErr:     "failed to create fileset reader: reader create err",
			originalFileSet: defaultOriginalFileSet, rollupOptions: defaultRollupOptions, nsOpts: mockNsOpts, fsOpts: mockFsOpts,
		},
		{
			name: "writer create failure",
			setupReader:     func(r *mockFsReader, _ []ts.Datapoint) {
				r.openFn = func(o fs.DataReaderOpenOptions) error { return nil }
				r.streamingReadFn = func() (fs.FileSetDataEntry, error) { return fs.FileSetDataEntry{}, io.EOF }
				r.closeFn = func() error { return nil }
			},
			setupMetrics: func(m *MockMetrics) {
				m.EXPECT().IncActive(); m.EXPECT().DecActive(); m.EXPECT().ReportAttempt(); m.EXPECT().ReportFailure("writer_open_create")
			},
			expectedErr:     "failed to create streaming writer for rollup: writer create err",
			originalFileSet: defaultOriginalFileSet, rollupOptions: defaultRollupOptions, nsOpts: mockNsOpts, fsOpts: mockFsOpts,
		},
		{
			name: "reader open failure",
			setupReader:     func(r *mockFsReader, _ []ts.Datapoint) { r.openFn = func(o fs.DataReaderOpenOptions) error { return fmt.Errorf("reader open err") } },
			setupMetrics: func(m *MockMetrics) {
				m.EXPECT().IncActive(); m.EXPECT().DecActive(); m.EXPECT().ReportAttempt(); m.EXPECT().ReportFailure("reader_open")
			},
			expectedErr:     "failed to open reader for original fileset: reader open err",
			originalFileSet: defaultOriginalFileSet, rollupOptions: defaultRollupOptions, nsOpts: mockNsOpts, fsOpts: mockFsOpts,
		},
		{
			name: "writer open failure",
			setupReader: func(r *mockFsReader, _ []ts.Datapoint) {
				r.openFn = func(o fs.DataReaderOpenOptions) error { return nil }
				r.streamingReadFn = func() (fs.FileSetDataEntry, error) { return fs.FileSetDataEntry{}, io.EOF }
				r.closeFn = func() error { return nil }
			},
			setupWriter: func(w *mockFsStreamingWriter, _ []ts.Datapoint) {
				w.openFn = func(o fs.StreamingWriterOpenOptions) error { return fmt.Errorf("writer open err") }
				w.abortFn = func() error { return nil }
			},
			setupMetrics: func(m *MockMetrics) {
				m.EXPECT().IncActive(); m.EXPECT().DecActive(); m.EXPECT().ReportAttempt(); m.EXPECT().ReportFailure("writer_open")
			},
			expectedErr:     "failed to open writer for rollup fileset",
			originalFileSet: defaultOriginalFileSet, rollupOptions: defaultRollupOptions, nsOpts: mockNsOpts, fsOpts: mockFsOpts,
		},
		{
			name: "series read error",
			setupReader: func(r *mockFsReader, _ []ts.Datapoint) {
				r.openFn = func(o fs.DataReaderOpenOptions) error { return nil }
				r.streamingReadFn = func() (fs.FileSetDataEntry, error) { return fs.FileSetDataEntry{}, fmt.Errorf("series read err") }
				r.closeFn = func() error { return nil }
			},
			setupWriter: func(w *mockFsStreamingWriter, _ []ts.Datapoint) {
				w.openFn = func(o fs.StreamingWriterOpenOptions) error { return nil } 
				w.abortFn = func() error { return nil }
			},
			setupMetrics: func(m *MockMetrics) {
				m.EXPECT().IncActive(); m.EXPECT().DecActive(); m.EXPECT().ReportAttempt(); m.EXPECT().ReportFailure("series_read")
			},
			expectedErr:     "error reading from original fileset: series read err",
			originalFileSet: defaultOriginalFileSet, rollupOptions: defaultRollupOptions, nsOpts: mockNsOpts, fsOpts: mockFsOpts,
		},
		{
			name: "series write error",
			testData: []ts.Datapoint{{Timestamp: xtime.FromSeconds(0), Value: 1.0}}, 
			expectedAggData: []ts.Datapoint{{Timestamp: expectedNewBlockStart, Value: 1.0}},
			setupReader: func(r *mockFsReader, data []ts.Datapoint) {
				r.openFn = func(o fs.DataReaderOpenOptions) error { return nil }
				tempEncoder := tsenc.NewEncoder(expectedNewBlockStart, nil, tsenc.DefaultEncodingOptions, mockSchema)
				for _, dp := range data { tempEncoder.Encode(dp, xtime.Second, nil) }
				seg := tempEncoder.Discard(); var rawData []byte
				if seg.Head != nil { rawData = append(rawData, seg.Head.Bytes()...) }
				if seg.Tail != nil { rawData = append(rawData, seg.Tail.Bytes()...) }
				readCallCount := 0
				r.streamingReadFn = func() (fs.FileSetDataEntry, error) {
					if readCallCount == 0 {
						readCallCount++; return fs.FileSetDataEntry{ID: ident.StringID("foo"), Data: rawData}, nil
					}
					return fs.FileSetDataEntry{}, io.EOF
				}
				r.closeFn = func() error { return nil }
			},
			setupReaderIter: func(iter *encoding.MockReaderIterator, data []ts.Datapoint) {
				dpIdx := -1
				iter.EXPECT().Next().DoAndReturn(func() bool { dpIdx++; return dpIdx < len(data) }).AnyTimes()
				iter.EXPECT().Current().DoAndReturn(func() (ts.Datapoint, xtime.Unit, ts.Annotation) { return data[dpIdx], xtime.Second, nil }).AnyTimes()
				iter.EXPECT().Reset(gomock.Any(), gomock.Any()).AnyTimes()
				iter.EXPECT().Err().Return(nil).AnyTimes()
			},
			setupWriter: func(w *mockFsStreamingWriter, _ []ts.Datapoint) {
				w.openFn = func(o fs.StreamingWriterOpenOptions) error { return nil }
				w.writeFn = func(id ident.ID, tags ident.TagIterator, data [][]byte, checksum uint32) error { return fmt.Errorf("series write err") }
				w.abortFn = func() error { return nil }
			},
			setupEncoder: func(enc *encoding.MockEncoder, aggData []ts.Datapoint) {
				enc.EXPECT().Reset(expectedNewBlockStart, len(aggData), mockSchema).Times(1)
				for _, dp := range aggData {
					enc.EXPECT().Encode(dp, xtime.Nanosecond, dp.Annotation).Return(nil).Times(1)
				}
				failedWriteSegmentEncoder := tsenc.NewEncoder(expectedNewBlockStart, nil, tsenc.DefaultEncodingOptions, mockSchema)
				for _, dp := range aggData {	failedWriteSegmentEncoder.Encode(dp, xtime.Nanosecond, dp.Annotation) }
				mockEncoder.EXPECT().Discard().Return(failedWriteSegmentEncoder.Discard()).Times(1)
			},
			setupMetrics: func(m *MockMetrics) {
				m.EXPECT().IncActive(); m.EXPECT().DecActive(); m.EXPECT().ReportAttempt()
				m.EXPECT().ReportSeriesRead(int64(1))
				m.EXPECT().ReportDataReadBytes(gomock.Any())
				m.EXPECT().ReportFailure("series_write")
			},
			expectedErr:     "failed to write series foo to rollup fileset: series write err",
			originalFileSet: defaultOriginalFileSet, rollupOptions: defaultRollupOptions, nsOpts: mockNsOpts, fsOpts: mockFsOpts,
		},
		{
			name: "writer close error",
			setupReader: func(r *mockFsReader, _ []ts.Datapoint) {
				r.openFn = func(o fs.DataReaderOpenOptions) error { return nil }
				r.streamingReadFn = func() (fs.FileSetDataEntry, error) { return fs.FileSetDataEntry{}, io.EOF }
				r.closeFn = func() error { return nil }
			},
			setupReaderIter: func(iter *encoding.MockReaderIterator, _ []ts.Datapoint){
				iter.EXPECT().Next().Return(false).AnyTimes() 
				iter.EXPECT().Reset(gomock.Any(), gomock.Any()).AnyTimes()
				iter.EXPECT().Err().Return(nil).AnyTimes()
			},
			setupWriter: func(w *mockFsStreamingWriter, _ []ts.Datapoint) {
				w.openFn = func(o fs.StreamingWriterOpenOptions) error { return nil }
				w.writeFn = func(id ident.ID, tags ident.TagIterator, data [][]byte, checksum uint32) error { return nil }
				w.closeFn = func() error { return fmt.Errorf("writer close err") }
			},
			setupMetrics: func(m *MockMetrics) {
				m.EXPECT().IncActive(); m.EXPECT().DecActive(); m.EXPECT().ReportAttempt()
				m.EXPECT().ReportSeriesRead(int64(0)); m.EXPECT().ReportSeriesWritten(int64(0))
				m.EXPECT().ReportDataReadBytes(int64(0)); m.EXPECT().ReportDataWrittenBytes(int64(0))
				m.EXPECT().ReportFailure("writer_finalize")
			},
			expectedErr:     "failed to close rollup writer: writer close err",
			originalFileSet: defaultOriginalFileSet, rollupOptions: defaultRollupOptions, nsOpts: mockNsOpts, fsOpts: mockFsOpts,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockReader := newMockFsReader(ctrl)
			mockWriter := newMockFsStreamingWriter(ctrl)

			// Note: testData and expectedAggData might be nil for tests not needing them.
			// setupReader, setupWriter, etc., should handle nil data if necessary or be omitted in test cases.
			if tc.setupReader != nil {
				tc.setupReader(mockReader, tc.testData)
			}
			if tc.setupWriter != nil {
				tc.setupWriter(mockWriter, tc.expectedAggData)
			}
			if tc.setupMetrics != nil {
				tc.setupMetrics(mockRollupMetrics)
			}
			if tc.setupEncoder != nil {
				tc.setupEncoder(mockEncoder, tc.expectedAggData)
			}
			if tc.setupReaderIter != nil {
				tc.setupReaderIter(mockReaderIter, tc.testData)
			}

			currentNewReaderFn := mockNewReaderFactory.EXPECT().Execute(gomock.Any(), gomock.Any()).Return(mockReader, nil)
			currentNewWriterFn := mockNewWriterFactory.EXPECT().Execute(gomock.Any()).Return(mockWriter, nil)

			if tc.name == "reader create failure" {
				currentNewReaderFn.Return(nil, fmt.Errorf("reader create err"))
			}
			if tc.name == "writer create failure" {
				currentNewReaderFn.Return(mockReader, nil)
				currentNewWriterFn.Return(nil, fmt.Errorf("writer create err"))
			}
			
			if tc.name != "reader create failure" && tc.name != "writer create failure" {
				currentNewReaderFn.AnyTimes()
				currentNewWriterFn.AnyTimes()
			}

			err := RollupFileSet(tc.originalFileSet, tc.rollupOptions, tc.nsOpts, tc.fsOpts, mockRollupMetrics,
				mockNewReaderFactory, mockNewWriterFactory)

			if tc.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
