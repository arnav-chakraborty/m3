package m3tsz

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

import (
	"testing"
	"time"
	"math"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var (
	testStartTime = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	testOpts      = encoding.NewOptions() // Use default options for now
)

// Helper to create a new encoder and decoder for tests
func newTestEncoderDecoder() (encoding.Encoder, encoding.Decoder) {
	// Simple byte pool for tests
	bytesPool := pool.NewCheckedBytesPool(nil, pool.NewObjectPoolOptions().SetSize(1), func(s [][]byte) pool.CheckedBytesPool {
		return pool.NewCheckedBytesPool(s, pool.NewObjectPoolOptions().SetSize(1), nil)
	})
	bytesPool.Init()

	encOpts := testOpts.SetEncoderPool(pool.NewEncoderPool(nil))
	encOpts.EncoderPool().Init(func() encoding.Encoder {
		return NewM3TSZAdvancedEncoder(time.Time{}, bytesPool, encOpts)
	})


	decOpts := testOpts.SetReaderIteratorPool(pool.NewReaderIteratorPool(nil))
	decOpts.ReaderIteratorPool().Init(func(r xio.Reader64, s encoding.Schema) encoding.ReaderIterator {
		return NewM3TSZAdvancedDecoder(bytesPool, decOpts) // Decoder is the iterator
	})

	encoder := NewM3TSZAdvancedEncoder(testStartTime, bytesPool, encOpts)
	decoder := NewM3TSZAdvancedDecoder(bytesPool, decOpts)
	return encoder, decoder
}

// Helper to run encode/decode cycle
func runEncodeDecodeCycle(t *testing.T, dps []ts.Datapoint, initialStartTime time.Time) []ts.Datapoint {
	encoder, decoder := newTestEncoderDecoder()
	encoder.Reset(initialStartTime, len(dps), testOpts)

	for _, dp := range dps {
		err := encoder.Encode(dp, xtime.Nanosecond, nil) // Assuming Nanosecond unit, nil annotation
		require.NoError(t, err)
	}

	seg, err := encoder.Stream(nil) // No block reader needed for this encoder type
	require.NoError(t, err)
	require.NotNil(t, seg)

	// Create a SegmentReader from the segment
	segmentReader := xio.NewSegmentReader(seg)

	// Use the Decode method of the decoder to get a SeriesIterator
	seriesIter, err := decoder.Decode(segmentReader)
	require.NoError(t, err)
	require.NotNil(t, seriesIter)

	var decodedDps []ts.Datapoint
	for seriesIter.Next() {
		dp, unit, annotation := seriesIter.Current()
		// For simplicity in these tests, we assume unit and annotation are not the primary focus of correctness
		// unless specified by a test.
		_ = unit
		_ = annotation
		decodedDps = append(decodedDps, dp)
	}
	require.NoError(t, seriesIter.Err())
	seriesIter.Close() // Close the iterator which also closes the decoder

	return decodedDps
}

// Helper to compare datapoint slices
func requireDatapointsEqual(t *testing.T, expected, actual []ts.Datapoint) {
	require.Equal(t, len(expected), len(actual), "Number of datapoints mismatch")
	for i := range expected {
		require.True(t, expected[i].Timestamp.Equal(actual[i].Timestamp), "Timestamp mismatch at index %d. Expected %v, got %v", i, expected[i].Timestamp, actual[i].Timestamp)
		// Handle NaN comparisons for float values
		if math.IsNaN(expected[i].Value) {
			require.True(t, math.IsNaN(actual[i].Value), "Value mismatch at index %d. Expected NaN, got %f", i, actual[i].Value)
		} else {
			require.InDelta(t, expected[i].Value, actual[i].Value, 1e-9, "Value mismatch at index %d. Expected %f, got %f", i, expected[i].Value, actual[i].Value)
		}
	}
}

// --- Timestamp Encoding/Decoding Tests ---

func TestTimestamp_SmallDeltas(t *testing.T) {
	dps := []ts.Datapoint{
		{Timestamp: testStartTime.Add(10 * time.Second), Value: 1.0},
		{Timestamp: testStartTime.Add(20 * time.Second), Value: 2.0},
		{Timestamp: testStartTime.Add(30 * time.Second), Value: 3.0},
	}
	decoded := runEncodeDecodeCycle(t, dps, testStartTime)
	requireDatapointsEqual(t, dps, decoded)
}

func TestTimestamp_LargeDeltas(t *testing.T) {
	dps := []ts.Datapoint{
		{Timestamp: testStartTime.Add(1000 * time.Hour), Value: 1.0},
		{Timestamp: testStartTime.Add(2000 * time.Hour), Value: 2.0},
		{Timestamp: testStartTime.Add(3000 * time.Hour), Value: 3.0},
	}
	decoded := runEncodeDecodeCycle(t, dps, testStartTime)
	requireDatapointsEqual(t, dps, decoded)
}

func TestTimestamp_RLE(t *testing.T) {
	delta := 15 * time.Second
	dps := []ts.Datapoint{
		{Timestamp: testStartTime.Add(delta), Value: 1.0},
		{Timestamp: testStartTime.Add(2 * delta), Value: 2.0}, // Delta = delta
		{Timestamp: testStartTime.Add(3 * delta), Value: 3.0}, // Delta = delta
		{Timestamp: testStartTime.Add(4 * delta), Value: 4.0}, // Delta = delta
		{Timestamp: testStartTime.Add(5*delta + 1*time.Minute), Value: 5.0}, // Different delta
		{Timestamp: testStartTime.Add(6*delta + 1*time.Minute), Value: 6.0}, // Delta = delta (new run)
	}
	decoded := runEncodeDecodeCycle(t, dps, testStartTime)
	requireDatapointsEqual(t, dps, decoded)
}

func TestTimestamp_MixDeltasAndRLE(t *testing.T) {
    dps := []ts.Datapoint{
        {Timestamp: testStartTime.Add(10 * time.Second), Value: 1},
        {Timestamp: testStartTime.Add(25 * time.Second), Value: 2}, // Delta: 15s
        {Timestamp: testStartTime.Add(40 * time.Second), Value: 3}, // Delta: 15s (RLE starts)
        {Timestamp: testStartTime.Add(55 * time.Second), Value: 4}, // Delta: 15s
        {Timestamp: testStartTime.Add(100 * time.Second), Value: 5},// Delta: 45s (breaks RLE)
        {Timestamp: testStartTime.Add(120 * time.Second), Value: 6},// Delta: 20s
        {Timestamp: testStartTime.Add(140 * time.Second), Value: 7},// Delta: 20s (RLE starts)
    }
    decoded := runEncodeDecodeCycle(t, dps, testStartTime)
    requireDatapointsEqual(t, dps, decoded)
}


func TestTimestamp_Edge_EmptyStream(t *testing.T) {
	dps := []ts.Datapoint{}
	decoded := runEncodeDecodeCycle(t, dps, testStartTime)
	requireDatapointsEqual(t, dps, decoded)
}

func TestTimestamp_Edge_SingleDatapoint(t *testing.T) {
	dps := []ts.Datapoint{
		{Timestamp: testStartTime.Add(10 * time.Second), Value: 1.0},
	}
	decoded := runEncodeDecodeCycle(t, dps, testStartTime)
	requireDatapointsEqual(t, dps, decoded)
}

// --- Value Encoding/Decoding Tests ---

func TestValue_SmallXOR(t *testing.T) {
	dps := []ts.Datapoint{
		{Timestamp: testStartTime.Add(1 * time.Second), Value: 1.001},
		{Timestamp: testStartTime.Add(2 * time.Second), Value: 1.002},
		{Timestamp: testStartTime.Add(3 * time.Second), Value: 1.003},
	}
	decoded := runEncodeDecodeCycle(t, dps, testStartTime)
	requireDatapointsEqual(t, dps, decoded)
}

func TestValue_LargeXOR(t *testing.T) {
	dps := []ts.Datapoint{
		{Timestamp: testStartTime.Add(1 * time.Second), Value: 1.0e6},
		{Timestamp: testStartTime.Add(2 * time.Second), Value: -2.0e6},
		{Timestamp: testStartTime.Add(3 * time.Second), Value: 3.0e7},
	}
	decoded := runEncodeDecodeCycle(t, dps, testStartTime)
	requireDatapointsEqual(t, dps, decoded)
}

func TestValue_RLE(t *testing.T) {
	dps := []ts.Datapoint{
		{Timestamp: testStartTime.Add(1 * time.Second), Value: 123.456},
		{Timestamp: testStartTime.Add(2 * time.Second), Value: 123.456}, // RLE
		{Timestamp: testStartTime.Add(3 * time.Second), Value: 123.456}, // RLE
		{Timestamp: testStartTime.Add(4 * time.Second), Value: 789.012}, // Breaks RLE
		{Timestamp: testStartTime.Add(5 * time.Second), Value: 789.012}, // RLE
	}
	decoded := runEncodeDecodeCycle(t, dps, testStartTime)
	requireDatapointsEqual(t, dps, decoded)
}

func TestValue_LZTZChanges(t *testing.T) {
    // Values designed to change leading/trailing zeros in XOR
    v1 := 1.0      // 0x3ff0000000000000
    v2 := 2.0      // 0x4000000000000000
    v3 := 0.5      // 0x3fe0000000000000
    v4 := 1.000000000000001 // 0x3ff0000000000001 (subtle change)
    dps := []ts.Datapoint{
        {Timestamp: testStartTime.Add(1 * time.Second), Value: v1},
        {Timestamp: testStartTime.Add(2 * time.Second), Value: v2}, // XOR will have specific LZ/TZ
        {Timestamp: testStartTime.Add(3 * time.Second), Value: v3}, // XOR with v2 will change LZ/TZ significantly
        {Timestamp: testStartTime.Add(4 * time.Second), Value: v4}, // XOR with v3, again different
    }
    decoded := runEncodeDecodeCycle(t, dps, testStartTime)
    requireDatapointsEqual(t, dps, decoded)
}

func TestValue_Edge_EmptyStream(t *testing.T) {
	// Same as timestamp empty stream, but focus is on value handling
	dps := []ts.Datapoint{}
	decoded := runEncodeDecodeCycle(t, dps, testStartTime)
	requireDatapointsEqual(t, dps, decoded)
}

func TestValue_Edge_SingleDatapoint(t *testing.T) {
	// Same as timestamp single, focus on value
	dps := []ts.Datapoint{
		{Timestamp: testStartTime.Add(1 * time.Second), Value: 123.456},
	}
	decoded := runEncodeDecodeCycle(t, dps, testStartTime)
	requireDatapointsEqual(t, dps, decoded)
}

func TestValue_NaN(t *testing.T) {
    dps := []ts.Datapoint{
        {Timestamp: testStartTime.Add(1 * time.Second), Value: 1.0},
        {Timestamp: testStartTime.Add(2 * time.Second), Value: math.NaN()},
        {Timestamp: testStartTime.Add(3 * time.Second), Value: 2.0},
        {Timestamp: testStartTime.Add(4 * time.Second), Value: math.NaN()},
        {Timestamp: testStartTime.Add(5 * time.Second), Value: math.NaN()}, // RLE of NaN
    }
    decoded := runEncodeDecodeCycle(t, dps, testStartTime)
    requireDatapointsEqual(t, dps, decoded)
}


// --- Combined Timestamp and Value Encoding/Decoding Tests ---

func TestCombined_RealisticData(t *testing.T) {
	dps := []ts.Datapoint{
		{Timestamp: testStartTime.Add(10 * time.Second), Value: 100.5},
		{Timestamp: testStartTime.Add(20 * time.Second), Value: 100.5}, // Value RLE
		{Timestamp: testStartTime.Add(30 * time.Second), Value: 101.0}, // Ts delta same, val diff
		{Timestamp: testStartTime.Add(45 * time.Second), Value: 101.0}, // Ts delta diff, val RLE from prev
		{Timestamp: testStartTime.Add(60 * time.Second), Value: 101.0}, // Ts delta same (15s), val RLE
		{Timestamp: testStartTime.Add(70 * time.Second), Value: 102.0}, // Ts delta diff (10s), val diff
	}
	decoded := runEncodeDecodeCycle(t, dps, testStartTime)
	requireDatapointsEqual(t, dps, decoded)
}

// --- Comparison with Original M3TSZ (Placeholders) ---

func TestCompareWithM3TSZ_CompressionRatio(t *testing.T) {
	// Placeholder: This test would encode a dataset with both M3TSZ-Advanced
	// and the original M3TSZ, then compare the byte lengths of the output.
	// Requires access to original M3TSZ encoder and same dataset.
	t.Skip("Placeholder: M3TSZ comparison for compression ratio not implemented")
}

func TestCompareWithM3TSZ_Performance(t *testing.T) {
	// Placeholder: This test would involve benchmarking encoding and decoding speeds
	// for both M3TSZ-Advanced and original M3TSZ on a representative dataset.
	t.Skip("Placeholder: M3TSZ comparison for performance not implemented")
}

// --- Basic Encoder/Decoder Lifecycle Tests ---

func TestLifecycle_ResetEncoder(t *testing.T) {
	encoder, _ := newTestEncoderDecoder()

	dps1 := []ts.Datapoint{{Timestamp: testStartTime.Add(10*time.Second), Value: 1.0}}
	encoder.Reset(testStartTime, len(dps1), testOpts)
	for _, dp := range dps1 { require.NoError(t, encoder.Encode(dp, xtime.Nanosecond, nil)) }
	s1, err := encoder.Stream(nil); require.NoError(t, err); require.NotZero(t, s1.Len())

	dps2 := []ts.Datapoint{{Timestamp: testStartTime.Add(100*time.Second), Value: 10.0}}
	encoder.Reset(testStartTime.Add(1*time.Hour), len(dps2), testOpts) // Reset with new start time
	for _, dp := range dps2 { require.NoError(t, encoder.Encode(dp, xtime.Nanosecond, nil)) }
	s2, err := encoder.Stream(nil); require.NoError(t, err); require.NotZero(t, s2.Len())

	// Basic check: ensure the segments are different (Reset had an effect)
	// This isn't a deep check of correctness of content, more that Reset allows reuse.
	require.NotEqual(t, s1.Bytes(), s2.Bytes(), "Segments should be different after reset")
	encoder.Close()
}


func TestLifecycle_ResetDecoder(t *testing.T) {
    encoder, decoder := newTestEncoderDecoder()

    // First set of data
    dps1 := []ts.Datapoint{{Timestamp: testStartTime.Add(10 * time.Second), Value: 1.0}}
    encoder.Reset(testStartTime, len(dps1), testOpts)
    for _, dp := range dps1 { require.NoError(t, encoder.Encode(dp, xtime.Nanosecond, nil)) }
    seg1, _ := encoder.Stream(nil)

    iter1, _ := decoder.Decode(xio.NewSegmentReader(seg1))
    count1 := 0; for iter1.Next() { count1++ }; require.NoError(t, iter1.Err())
    require.Equal(t, len(dps1), count1)
    iter1.Close()

    // Second set of data (different start time for encoder to make stream different)
    dps2 := []ts.Datapoint{
        {Timestamp: testStartTime.Add(1 * time.Hour).Add(20 * time.Second), Value: 2.0},
        {Timestamp: testStartTime.Add(1 * time.Hour).Add(30 * time.Second), Value: 3.0},
    }
    encoder.Reset(testStartTime.Add(1*time.Hour), len(dps2), testOpts)
    for _, dp := range dps2 { require.NoError(t, encoder.Encode(dp, xtime.Nanosecond, nil)) }
    seg2, _ := encoder.Stream(nil)

    // Reset decoder with new data
    decoder.Reset(xio.NewSegmentReader(seg2), nil) // schema is nil for m3tsz
    count2 := 0; for decoder.Next() { count2++ }; require.NoError(t, decoder.Err())
    require.Equal(t, len(dps2), count2)
    decoder.Close() // Close decoder itself when used as iterator after Reset
}


func TestLifecycle_EncoderDiscard(t *testing.T) {
	encoder, _ := newTestEncoderDecoder()
	encoder.Reset(testStartTime, 10, testOpts)
	require.NoError(t, encoder.Encode(ts.Datapoint{Timestamp: testStartTime.Add(1 * time.Second), Value: 1.0}, xtime.Nanosecond, nil))
	require.NotZero(t, encoder.Len(), "Encoder length should be non-zero after encode")

	encoder.Discard()
	// M3TSZ ostream discard might not reset length to 0 if buffer comes from pool and is just reset.
	// The key is that a new Stream() after Discard would be empty or contain only trailer.
	// Let's check if we can encode again and get a valid small stream.
	encoder.Reset(testStartTime,10,testOpts) // Reset to a clean state
	require.NoError(t, encoder.Encode(ts.Datapoint{Timestamp: testStartTime.Add(2 * time.Second), Value: 2.0}, xtime.Nanosecond, nil))
	seg, err := encoder.Stream(nil)
	require.NoError(t, err)
	require.True(t, seg.Len() > 1, "Segment length should be > 1 (data + trailer)") // Has to be at least 1 byte trailer
	encoder.Close()
}

func TestLifecycle_EncoderLastEncodedTime(t *testing.T) {
    encoder, _ := newTestEncoderDecoder()
    encoder.Reset(testStartTime, 10, testOpts)

    lastTime := testStartTime.Add(50 * time.Second)
    dps := []ts.Datapoint{
        {Timestamp: testStartTime.Add(10 * time.Second), Value: 1.0},
        {Timestamp: lastTime, Value: 2.0},
    }
    for _, dp := range dps {
        require.NoError(t, encoder.Encode(dp, xtime.Nanosecond, nil))
    }

    encodedTime, err := encoder.LastEncodedTime()
    require.NoError(t, err)
    require.True(t, lastTime.Equal(encodedTime), "LastEncodedTime mismatch")
	encoder.Close()
}

// Test with a sequence that should trigger RLE for both timestamps and values
func TestCombined_TimestampAndValueRLE(t *testing.T) {
    tsDelta := 10 * time.Second
    val := 123.45
    dps := []ts.Datapoint{
        {Timestamp: testStartTime.Add(1 * tsDelta), Value: val},
        {Timestamp: testStartTime.Add(2 * tsDelta), Value: val}, // ts delta = 10s, val same
        {Timestamp: testStartTime.Add(3 * tsDelta), Value: val}, // ts delta = 10s, val same
        {Timestamp: testStartTime.Add(4 * tsDelta), Value: 999.9},// ts delta = 10s, val different
        {Timestamp: testStartTime.Add(5 * tsDelta), Value: 999.9},// ts delta = 10s, val same
        {Timestamp: testStartTime.Add(6*tsDelta + 5*time.Second), Value: val}, // ts delta = 15s, val different
    }
    decoded := runEncodeDecodeCycle(t, dps, testStartTime)
    requireDatapointsEqual(t, dps, decoded)
}
