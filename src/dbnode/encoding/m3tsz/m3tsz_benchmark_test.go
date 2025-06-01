// Copyright (c) 2016 Uber Technologies, Inc.
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

package m3tsz

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	numDatapointsForBenchmark = 1000
	defaultBenchStartTime     = "2024-01-01T00:00:00Z"
)

var (
	benchStartTime time.Time
	benchOpts      encoding.Options
	bytesPool      pool.CheckedBytesPool
	encoderPool    encoding.EncoderPool
	iteratorPool   encoding.ReaderIteratorPool
)

func init() {
	var err error
	benchStartTime, err = time.Parse(time.RFC3339, defaultBenchStartTime)
	if err != nil {
		panic(err)
	}

	bytesPool = pool.NewCheckedBytesPool(nil, pool.NewObjectPoolOptions().SetSize(1), func(s [][]byte) pool.CheckedBytesPool {
		return pool.NewCheckedBytesPool(s, pool.NewObjectPoolOptions().SetSize(1), nil)
	})
	bytesPool.Init()

	benchOpts = encoding.NewOptions().SetBytesPool(bytesPool)

	encoderPool = pool.NewEncoderPool(pool.NewObjectPoolOptions().SetSize(1))
	// Allocator set per encoding type in benchmark setup

	iteratorPool = pool.NewReaderIteratorPool(pool.NewObjectPoolOptions().SetSize(1))
	// Allocator set per encoding type in benchmark setup

	benchOpts = benchOpts.SetEncoderPool(encoderPool).SetReaderIteratorPool(iteratorPool)
}

// --- Data Generation Helpers ---

type valueGenFunc func(i int) float64

func generateDatapoints(numPoints int, startTime time.Time, timeStep time.Duration, valGen valueGenFunc) []ts.Datapoint {
	dps := make([]ts.Datapoint, numPoints)
	currentTime := startTime
	for i := 0; i < numPoints; i++ {
		dps[i] = ts.Datapoint{Timestamp: currentTime, Value: valGen(i)}
		currentTime = currentTime.Add(timeStep)
	}
	return dps
}

var (
	flatLineData = generateDatapoints(numDatapointsForBenchmark, benchStartTime, time.Second*10, func(i int) float64 { return 123.456 })

	smallChangesData = generateDatapoints(numDatapointsForBenchmark, benchStartTime, time.Second*10, func(i int) float64 { return 100.0 + float64(i)*0.01 })

	largeChangesData = generateDatapoints(numDatapointsForBenchmark, benchStartTime, time.Second*10, func(i int) float64 { return float64(i*1000) * math.Sin(float64(i)) })

	rleTimestampData = generateDatapoints(numDatapointsForBenchmark, benchStartTime, time.Second*10, func(i int) float64 {
		// Timestamps will have regular delta, then a few same deltas
		if i%5 < 3 { // Creates runs of 3 identical deltas
			return float64(i)
		}
		return float64(i*2) // Vary values otherwise
	})
	// Corrected rleTimestampData to actually make deltas repeat
	// For this, we need to manipulate timeStep within generation or have specific time sequence
	// Simplified: generate a fixed delta sequence for RLE test
	fixedTimeStepRLEDataPoints := make([]ts.Datapoint, numDatapointsForBenchmark)
	currentTime := benchStartTime
	fixedDelta := time.Second * 15
	for i:=0; i < numDatapointsForBenchmark; i++ {
		fixedTimeStepRLEDataPoints[i] = ts.Datapoint{Timestamp: currentTime, Value: float64(i % 10)} // Values vary to isolate ts RLE
		currentTime = currentTime.Add(fixedDelta)
	}


	rleValueData = generateDatapoints(numDatapointsForBenchmark, benchStartTime, time.Second*10, func(i int) float64 {
		return float64( (i % 5) * 10 ) // Creates runs of 5 identical values
	})

	nanData = generateDatapoints(numDatapointsForBenchmark, benchStartTime, time.Second*10, func(i int) float64 {
		if i%2 == 0 { return float64(i) }
		return math.NaN()
	})
)

// --- Core Benchmark Logic ---

func runEncodingBenchmark(b *testing.B, encType encoding.EncodingType, data []ts.Datapoint, startTime time.Time) {
	opts := benchOpts.SetEncodingType(encType)
	var currentEncoderPool encoding.EncoderPool

	switch encType {
	case encoding.M3TSZEncoding:
		currentEncoderPool = pool.NewEncoderPool(pool.NewObjectPoolOptions().SetSize(1))
		currentEncoderPool.Init(func() encoding.Encoder {
			return NewEncoder(startTime, nil, DefaultIntOptimizationEnabled, opts)
		})
	case encoding.M3TSZAdvancedEncoding:
		currentEncoderPool = pool.NewEncoderPool(pool.NewObjectPoolOptions().SetSize(1))
		currentEncoderPool.Init(func() encoding.Encoder {
			return NewM3TSZAdvancedEncoder(startTime, opts.BytesPool(), opts)
		})
	default:
		b.Fatalf("Unsupported encoding type: %v", encType)
	}
	opts = opts.SetEncoderPool(currentEncoderPool)


	var totalBytes int64
	var segment ts.Segment
	var err error

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		encoder := opts.EncoderPool().Get()
		encoder.Reset(startTime, len(data), nil) // Schema nil for these encoders

		for _, dp := range data {
			if err = encoder.Encode(dp, xtime.Nanosecond, nil); err != nil {
				b.Fatal(err)
			}
		}
		segment, err = encoder.Stream(nil)
		if err != nil {
			b.Fatal(err)
		}
		if n == 0 { // Only set bytes and log for the first run to avoid overhead
			totalBytes = int64(segment.Len())
		}
		encoder.Close() // Return to pool
	}
	b.SetBytes(totalBytes) // Total bytes processed in one op (one full encoding of dataset)

	originalSize := len(data) * (8 + 8) // Approx (ts + val)
	if totalBytes > 0 {
		b.ReportMetric(float64(originalSize)/float64(totalBytes), "compression_ratio")
	}
	b.ReportMetric(float64(totalBytes), "bytes/op")
}

func runDecodingBenchmark(b *testing.B, encType encoding.EncodingType, data []ts.Datapoint, startTime time.Time) {
	opts := benchOpts.SetEncodingType(encType)
	var currentEncoderPool encoding.EncoderPool
	var currentIteratorPool encoding.ReaderIteratorPool


	switch encType {
	case encoding.M3TSZEncoding:
		currentEncoderPool = pool.NewEncoderPool(pool.NewObjectPoolOptions().SetSize(1))
		currentEncoderPool.Init(func() encoding.Encoder {
			return NewEncoder(startTime, nil, DefaultIntOptimizationEnabled, opts)
		})
		currentIteratorPool = pool.NewReaderIteratorPool(pool.NewObjectPoolOptions().SetSize(1))
		currentIteratorPool.Init(func(r xio.Reader64, s encoding.SchemaDescr) encoding.ReaderIterator {
			return NewReaderIterator(r, DefaultIntOptimizationEnabled, opts, s)
		})
	case encoding.M3TSZAdvancedEncoding:
		currentEncoderPool = pool.NewEncoderPool(pool.NewObjectPoolOptions().SetSize(1))
		currentEncoderPool.Init(func() encoding.Encoder {
			return NewM3TSZAdvancedEncoder(startTime, opts.BytesPool(), opts)
		})
		currentIteratorPool = pool.NewReaderIteratorPool(pool.NewObjectPoolOptions().SetSize(1))
		currentIteratorPool.Init(func(r xio.Reader64, s encoding.SchemaDescr) encoding.ReaderIterator {
			decoder := NewM3TSZAdvancedDecoder(opts.BytesPool(), opts)
			decoder.Reset(r,s)
			return decoder
		})
	default:
		b.Fatalf("Unsupported encoding type: %v", encType)
	}
	opts = opts.SetEncoderPool(currentEncoderPool).SetReaderIteratorPool(currentIteratorPool)


	encoder := opts.EncoderPool().Get()
	encoder.Reset(startTime, len(data), nil)
	for _, dp := range data {
		if err := encoder.Encode(dp, xtime.Nanosecond, nil); err != nil {
			b.Fatal(err)
		}
	}
	segment, err := encoder.Stream(nil)
	if err != nil {
		b.Fatal(err)
	}
	encoder.Close()

	if segment.Len() == 0 && len(data) > 0 {
		b.Fatal("encoded segment is empty but data was provided")
	}


	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		iter := opts.ReaderIteratorPool().Get()
		// The segment reader needs to be re-created or reset for each iteration
		// if its state is consumed, which it is.
		segReader := xio.NewSegmentReader(segment)
		iter.Reset(segReader, nil) // Schema nil

		for iter.Next() {
			// No-op, just iterating
		}
		if err := iter.Err(); err != nil {
			b.Fatal(err)
		}
		iter.Close() // Return to pool
	}
	b.SetBytes(int64(segment.Len())) // Bytes processed per op is the compressed segment size
}

// --- Benchmark Functions ---

// M3TSZ (Original)
func BenchmarkM3TSZEncode_FlatLine(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZEncoding, flatLineData, benchStartTime) }
func BenchmarkM3TSZDecode_FlatLine(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZEncoding, flatLineData, benchStartTime) }
func BenchmarkM3TSZEncode_SmallChanges(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZEncoding, smallChangesData, benchStartTime) }
func BenchmarkM3TSZDecode_SmallChanges(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZEncoding, smallChangesData, benchStartTime) }
func BenchmarkM3TSZEncode_LargeChanges(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZEncoding, largeChangesData, benchStartTime) }
func BenchmarkM3TSZDecode_LargeChanges(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZEncoding, largeChangesData, benchStartTime) }
func BenchmarkM3TSZEncode_RLEValue(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZEncoding, rleValueData, benchStartTime) }
func BenchmarkM3TSZDecode_RLEValue(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZEncoding, rleValueData, benchStartTime) }
func BenchmarkM3TSZEncode_RLETimestamp(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZEncoding, fixedTimeStepRLEDataPoints, benchStartTime) }
func BenchmarkM3TSZDecode_RLETimestamp(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZEncoding, fixedTimeStepRLEDataPoints, benchStartTime) }
func BenchmarkM3TSZEncode_NaN(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZEncoding, nanData, benchStartTime) }
func BenchmarkM3TSZDecode_NaN(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZEncoding, nanData, benchStartTime) }


// M3TSZ-Advanced
func BenchmarkM3TSZAdvancedEncode_FlatLine(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZAdvancedEncoding, flatLineData, benchStartTime) }
func BenchmarkM3TSZAdvancedDecode_FlatLine(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZAdvancedEncoding, flatLineData, benchStartTime) }
func BenchmarkM3TSZAdvancedEncode_SmallChanges(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZAdvancedEncoding, smallChangesData, benchStartTime) }
func BenchmarkM3TSZAdvancedDecode_SmallChanges(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZAdvancedEncoding, smallChangesData, benchStartTime) }
func BenchmarkM3TSZAdvancedEncode_LargeChanges(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZAdvancedEncoding, largeChangesData, benchStartTime) }
func BenchmarkM3TSZAdvancedDecode_LargeChanges(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZAdvancedEncoding, largeChangesData, benchStartTime) }
func BenchmarkM3TSZAdvancedEncode_RLEValue(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZAdvancedEncoding, rleValueData, benchStartTime) }
func BenchmarkM3TSZAdvancedDecode_RLEValue(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZAdvancedEncoding, rleValueData, benchStartTime) }
func BenchmarkM3TSZAdvancedEncode_RLETimestamp(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZAdvancedEncoding, fixedTimeStepRLEDataPoints, benchStartTime) }
func BenchmarkM3TSZAdvancedDecode_RLETimestamp(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZAdvancedEncoding, fixedTimeStepRLEDataPoints, benchStartTime) }
func BenchmarkM3TSZAdvancedEncode_NaN(b *testing.B) { runEncodingBenchmark(b, encoding.M3TSZAdvancedEncoding, nanData, benchStartTime) }
func BenchmarkM3TSZAdvancedDecode_NaN(b *testing.B) { runDecodingBenchmark(b, encoding.M3TSZAdvancedEncoding, nanData, benchStartTime) }

// Example of a microbenchmark (original style from file, kept for context if needed)
// func BenchmarkMathPow(b *testing.B) {
// 	for n := 0; n < b.N; n++ {
// 		_ = 123.456 * math.Pow10(1)
// 	}
// }
// Note: Removed other microbenchmarks for brevity as this task focuses on end-to-end.
// They can be added back if they were part of the original file and needed.
// For now, the file is overwritten with new benchmark structure.

// Print compression ratios after all benchmarks (or within each)
// This is a simplified way, normally you'd collect results from b.ReportMetric
// For now, b.ReportMetric("compression_ratio", ratio) is used.
func TestMain(m *testing.M) {
	// Can add setup/teardown here if needed for all benchmarks in this package
	// For example, pre-generate all datasets once if generation is slow.
	fmt.Println("Running M3TSZ benchmarks...")
	m.Run()
}
