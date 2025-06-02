package integration

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/instrument"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/index" // For index.FileSetIndexFileSuffix
	"github.com/m3db/m3/src/dbnode/storage/rollup" // Import the rollup package
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/m3ninx/doc"
	// "github.com/m3db/m3/src/m3ninx/idx" // Not directly used, but good for context
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestRollupIntegration_CreateAndVerifyRollupData(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Skip integration test in short mode.
	}

	// --------------------------------------------------------------------
	// Test Setup
	// --------------------------------------------------------------------
	var (
		originalRetention = 20 * time.Minute
		originalBlockSize = 5 * time.Minute
		rollupResolution  = 10 * time.Minute
		rollupNewTTL      = 1 * time.Hour
	)

	nsID := ident.StringID("testRollupNsWithData")
	nsOpts := namespace.NewOptions().
		SetRetentionOptions(
			retention.NewOptions().
				SetRetentionPeriod(originalRetention).
				SetBlockSize(originalBlockSize).
				SetBufferFuture(1*time.Minute).
				SetBufferPast(1*time.Minute),
		).
		SetIndexOptions(
			namespace.NewIndexOptions().SetEnabled(true).SetBlockSize(originalBlockSize),
		).
		SetCleanupEnabled(true).
		SetBootstrapEnabled(true)

	nsRollupRegOpts := namespace.NewRollupOptions().
		SetResolution(rollupResolution).
		SetNewTTL(rollupNewTTL)
	nsOpts = nsOpts.SetRollupOptions(nsRollupRegOpts)

	nsMetadata, err := namespace.NewMetadata(nsID, nsOpts)
	require.NoError(t, err)
	
	schemaDescr := nsMetadata.Schema()
	require.NotNil(t, schemaDescr, "Schema should be available from metadata")
	m3tszEncodingOpts := schemaDescr.EncoderOptions()


	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{nsMetadata}).
		SetWriteNewSeriesAsync(true)

	tempDataDir, err := os.MkdirTemp("", "rollup_integration_data_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDataDir)

	simulatedFsOpts := fs.NewOptions().
		SetFilePathPrefix(tempDataDir).
		SetWriterBufferSize(10). 
		SetDecodingOptions(msgpack.NewDecodingOptions()).
		SetInstrumentOptions(testOpts.InstrumentOptions())

	now := xtime.Now().Truncate(originalBlockSize)
	expiredBlockStart := now.Add(-2 * originalRetention) 
	shard := uint32(0)
	originalFilesetDir := fs.ShardDataDirPath(simulatedFsOpts.FilePathPrefix(), nsID, shard)
	err = os.MkdirAll(originalFilesetDir, simulatedFsOpts.NewDirectoryMode())
	require.NoError(t, err)

	// --------------------------------------------------------------------
	// Prepare Source Data and Fileset
	// --------------------------------------------------------------------
	seriesList := []struct {
		ID   string
		Tags map[string]string
		Data []ts.Datapoint
	}{
		{
			ID:   "series1",
			Tags: map[string]string{"tag1": "val1", "tag2": "s1val"},
			Data: []ts.Datapoint{
				{Timestamp: expiredBlockStart.Add(1 * time.Minute), Value: 1.0},
				{Timestamp: expiredBlockStart.Add(3 * time.Minute), Value: 2.0},
				{Timestamp: expiredBlockStart.Add(7 * time.Minute), Value: 3.0},  // Last in 0-10m window for series1
				{Timestamp: expiredBlockStart.Add(12 * time.Minute), Value: 4.0},
				{Timestamp: expiredBlockStart.Add(18 * time.Minute), Value: 5.0}, // Last in 10-20m window for series1
			},
		},
		{
			ID:   "series2",
			Tags: map[string]string{"tag1": "val2", "tag2": "s2val"},
			Data: []ts.Datapoint{
				{Timestamp: expiredBlockStart.Add(2 * time.Minute), Value: 10.0}, // Last in 0-10m window for series2
				{Timestamp: expiredBlockStart.Add(11 * time.Minute), Value: 20.0},// Last in 10-20m window for series2
				{Timestamp: expiredBlockStart.Add(22 * time.Minute), Value: 30.0},// Last in 20-30m window for series2
			},
		},
	}

	var (
		allSeriesDataBytes bytes.Buffer
		indexEntries       []schema.IndexEntry
		totalSourceEntries int64 = 0
	)
	
	enc := m3tsz.NewEncoder(expiredBlockStart, nil, m3tszEncodingOpts, schemaDescr)

	for i, s := range seriesList {
		seriesID := ident.StringID(s.ID)
		var tags ident.Tags
		for k, v := range s.Tags {
			tags = tags.Append(ident.StringTag(k,v))
		}
		
		dataOffset := int64(allSeriesDataBytes.Len())
		enc.Reset(expiredBlockStart, 0, schemaDescr) 
		for _, dp := range s.Data {
			require.NoError(t, enc.Encode(dp, xtime.Second, nil))
		}
		totalSourceEntries += int64(len(s.Data))
		encoded := enc.Discard()
		
		n, err := allSeriesDataBytes.Write(encoded.Head.Bytes())
		require.NoError(t, err)
		dataSize := int64(n)
		if encoded.Tail != nil {
			n, err = allSeriesDataBytes.Write(encoded.Tail.Bytes())
			require.NoError(t, err)
			dataSize += int64(n)
		}

		indexEntry := schema.IndexEntry{
			Index:      int64(i), 
			ID:         seriesID.Bytes(),
			Size:       dataSize,
			Offset:     dataOffset,
			Checksum:   digest.SegmentChecksum(encoded),
			EncodedTags: convert.TagsToEncodedTags(tags),
		}
		indexEntries = append(indexEntries, indexEntry)
	}

	indexPath := fs.FilesetPathFromTimeAndIndex(originalFilesetDir, expiredBlockStart, 0, fs.IndexFileSuffixV2)
	indexFd, err := os.Create(indexPath)
	require.NoError(t, err)
	msgpackEncoder := msgpack.NewEncoder()
	indexFileDigest := digest.NewStreamingDigest()

	idxInfo := schema.IndexInfo{
		BlockStart:    expiredBlockStart.Seconds(),
		BlockSize:     int64(originalBlockSize),
		Entries:       totalSourceEntries, 
		MajorVersion:  schema.MajorVersion,
		SummariesInfo: schema.SummariesInfo{Summaries: int64(len(indexEntries))},
		IndexInfo:     schema.IndexFileInfo{NumDocs: int64(len(indexEntries))},
	}
	require.NoError(t, msgpackEncoder.EncodeIndexInfo(idxInfo))
	_, err = indexFd.Write(msgpackEncoder.Bytes()); require.NoError(t, err)
	indexFileDigest.Write(msgpackEncoder.Bytes())
	msgpackEncoder.Reset()

	for _, entry := range indexEntries {
		require.NoError(t, msgpackEncoder.EncodeIndexEntry(entry))
		_, err = indexFd.Write(msgpackEncoder.Bytes()); require.NoError(t, err)
		indexFileDigest.Write(msgpackEncoder.Bytes())
		msgpackEncoder.Reset()
	}
	
	idxTrailer := schema.IndexTrailer{NumEntries: int64(len(indexEntries))}
	require.NoError(t, msgpackEncoder.EncodeIndexTrailer(idxTrailer))
	_, err = indexFd.Write(msgpackEncoder.Bytes()); require.NoError(t, err)
	indexFileDigest.Write(msgpackEncoder.Bytes())
	indexFd.Close()

	dataFilePath := fs.FilesetPathFromTimeAndIndex(originalFilesetDir, expiredBlockStart, 0, fs.DataFileSuffix)
	err = os.WriteFile(dataFilePath, allSeriesDataBytes.Bytes(), simulatedFsOpts.NewFileMode())
	require.NoError(t, err)

	infoFilePath := fs.FilesetPathFromTimeAndIndex(originalFilesetDir, expiredBlockStart, 0, fs.InfoFileSuffix)
	msgpackEncoder.Reset()
	require.NoError(t, msgpackEncoder.EncodeIndexInfo(idxInfo)) 
	infoFileBytes := msgpackEncoder.Bytes()
	err = os.WriteFile(infoFilePath, infoFileBytes, simulatedFsOpts.NewFileMode())
	require.NoError(t, err)

	digestFilePath := fs.FilesetPathFromTimeAndIndex(originalFilesetDir, expiredBlockStart, 0, fs.DigestFileSuffix)
	digestFd, err := os.Create(digestFilePath)
	require.NoError(t, err)
	dw := digest.NewWriter(digestFd)
	require.NoError(t, dw.WriteDigests(
		digest.Checksum(infoFileBytes),
		indexFileDigest.Sum32(),
		digest.Checksum(allSeriesDataBytes.Bytes()),
		0, 0,
	))
	digestFd.Close()
	
	checkpointFilePath := fs.FilesetPathFromTimeAndIndex(originalFilesetDir, expiredBlockStart, 0, fs.CheckpointFileSuffix)
	digestOfDigestContents, err := os.ReadFile(digestFilePath)
	require.NoError(t, err)
	err = fs.WriteCheckpointFile(checkpointFilePath, digest.Checksum(digestOfDigestContents), fs.DefaultCheckpointFileMode)
	require.NoError(t, err)

	summariesPath := fs.FilesetPathFromTimeAndIndex(originalFilesetDir, expiredBlockStart, 0, fs.SummariesFileSuffix)
	summariesFile, err := os.Create(summariesPath); require.NoError(t, err); summariesFile.Close()
	bloomPath := fs.FilesetPathFromTimeAndIndex(originalFilesetDir, expiredBlockStart, 0, fs.BloomFilterFileSuffix)
	bloomFile, err := os.Create(bloomPath); require.NoError(t, err); bloomFile.Close()

	originalVolumeInfo := persist.FileSetVolumeInfo{
		Namespace:   nsID, Shard: shard, BlockStart:  expiredBlockStart, VolumeIndex: 0,
	}

	// --------------------------------------------------------------------
	// Trigger Rollup
	// --------------------------------------------------------------------
	testScope := tally.NewTestScope("test", nil)
	rollupTestMetrics := rollup.NewMetrics(testScope.SubScope("rollup_test_data_verify"))
	
	concreteRollupOpts := rollup.NewOptions().
		SetResolution(nsRollupRegOpts.Resolution()).
		SetNewTTL(nsRollupRegOpts.NewTTL())
	require.NoError(t, concreteRollupOpts.Validate())

	err = rollup.RollupFileSet(
		originalVolumeInfo, concreteRollupOpts, nsOpts, simulatedFsOpts, rollupTestMetrics,
		rollup.DefaultNewReaderFn(), rollup.DefaultNewStreamingWriterFn(),
	)
	require.NoError(t, err, "RollupFileSet failed")

	// --------------------------------------------------------------------
	// Verify Rollup Fileset Creation and Metadata (Part 1 checks)
	// --------------------------------------------------------------------
	rollupFilesetParentDir := filepath.Join(
		fs.ShardDataDirPath(simulatedFsOpts.FilePathPrefix(), nsID, shard),
		rollup.RollupDirName,
		rollupResolution.String(),
	)
	rollupFilesetActualDir := filepath.Join(rollupFilesetParentDir, "rollup_data", "0")
	expectedRollupBlockStart := expiredBlockStart.Truncate(rollupResolution)
	
	expectedFileTypes := []string{
		fs.InfoFileSuffix, fs.DataFileSuffix, fs.IndexFileSuffixV2,
		fs.SummariesFileSuffix, fs.BloomFilterFileSuffix,
		fs.DigestFileSuffix, fs.CheckpointFileSuffix,
	}
	for _, suffix := range expectedFileTypes {
		fPath := fs.FilesetPathFromTimeAndIndex(rollupFilesetActualDir, expectedRollupBlockStart, 0, suffix)
		_, err := os.Stat(fPath)
		require.NoError(t, err, "expected rollup file not found: %s", fPath)
	}

	rollupInfoFilePath := fs.FilesetPathFromTimeAndIndex(rollupFilesetActualDir, expectedRollupBlockStart, 0, fs.InfoFileSuffix)
	infoFileBytesRead, err := os.ReadFile(rollupInfoFilePath)
	require.NoError(t, err)
	decoder := msgpack.NewDecoder(simulatedFsOpts.DecodingOptions())
	decoder.Reset(msgpack.NewByteDecoderStream(infoFileBytesRead))
	rollupInfo, err := decoder.DecodeIndexInfo()
	require.NoError(t, err)
	require.Equal(t, expectedRollupBlockStart.Seconds(), rollupInfo.BlockStart)
	require.Equal(t, int64(rollupResolution), rollupInfo.BlockSize)
	
	// --------------------------------------------------------------------
	// Verify Aggregated Data Content
	// --------------------------------------------------------------------
	rollupIndexReaderOpts := fs.NewOptions().SetFilePathPrefix(rollupFilesetParentDir).SetDecodingOptions(msgpack.NewDecodingOptions())
	rollupIndexReaderConcrete, err := fs.NewIndexReader(rollupIndexReaderOpts)
	require.NoError(t, err)

	err = rollupIndexReaderConcrete.Open(fs.IndexReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   ident.StringID("rollup_data"), 
			Shard: 0, 
			BlockStart:  expectedRollupBlockStart, 
			VolumeIndex: 0,
		},
		FileSetType: persist.FileSetFlushType,
	})
	require.NoError(t, err)
	defer rollupIndexReaderConcrete.Close()

	rollupDataReader, err := fs.NewReader(nil, rollupIndexReaderOpts) 
	require.NoError(t, err)
	err = rollupDataReader.Open(fs.DataReaderOpenOptions{
		Identifier:  fs.FileSetFileIdentifier{
			Namespace:   ident.StringID("rollup_data"), Shard: 0, BlockStart:  expectedRollupBlockStart, VolumeIndex: 0,
		},
		FileSetType: persist.FileSetFlushType,
	})
	require.NoError(t, err)
	defer rollupDataReader.Close()

	expectedAggregatedSeries := map[string][]ts.Datapoint{
		"series1": {
			{Timestamp: expectedRollupBlockStart, Value: 3.0},                            
			{Timestamp: expectedRollupBlockStart.Add(rollupResolution), Value: 5.0},     
		},
		"series2": {
			{Timestamp: expectedRollupBlockStart, Value: 10.0},                           
			{Timestamp: expectedRollupBlockStart.Add(rollupResolution), Value: 20.0},    
			{Timestamp: expectedRollupBlockStart.Add(2 * rollupResolution), Value: 30.0},
		},
	}

	foundSeriesCount := 0
	for _, indexEntry := range rollupIndexReaderConcrete.Entries() {
		seriesID := ident.BytesID(indexEntry.ID)
		foundSeriesCount++

		expectedDps, ok := expectedAggregatedSeries[seriesID.String()]
		require.True(t, ok, "unexpected series in rollup index: %s", seriesID.String())

		dataSegment, err := rollupDataReader.ReadData(indexEntry.Offset, indexEntry.Size, indexEntry.Checksum)
		require.NoError(t, err)
		
		iter := m3tsz.NewReaderIterator(xio.NewSegmentReader(dataSegment), schemaDescr.EncoderOptions(), encoding.NewOptions())
		var decodedDps []ts.Datapoint
		for iter.Next() {
			dp, _, ann := iter.Current()
			if len(ann) > 0 { 
				dp.Annotation = append(dp.Annotation[:0:0], ann...)
			}
			decodedDps = append(decodedDps, dp)
		}
		require.NoError(t, iter.Err())
		iter.Close()
		
		require.Equal(t, len(expectedDps), len(decodedDps), "series %s: num DPs mismatch. Expected %v, Got %v", seriesID.String(), expectedDps, decodedDps)
		for i := range expectedDps {
			require.True(t, expectedDps[i].Timestamp.Equal(decodedDps[i].Timestamp),
				"series %s, dp %d: timestamp mismatch. Expected %s, Got %s", seriesID.String(), i, expectedDps[i].Timestamp, decodedDps[i].Timestamp)
			require.InDelta(t, expectedDps[i].Value, decodedDps[i].Value, 0.00001,
				"series %s, dp %d: value mismatch. Expected %f, Got %f", seriesID.String(), i, expectedDps[i].Value, decodedDps[i].Value)
		}
	}
	require.Equal(t, len(expectedAggregatedSeries), foundSeriesCount, "mismatch in number of series rolled up")
}
