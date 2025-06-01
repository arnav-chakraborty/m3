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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

type m3tszAdvancedDecoder struct {
	bitStream         *istream
	prevTimestamp     time.Time
	prevDelta         time.Duration
	adaptiveDeltaSize int
	err               error
	opts              encoding.Options
	bytesPool         pool.CheckedBytesPool
	prevAnnotation    []byte

	rleTimestampActive      bool
	rleTimestampDelta       time.Duration
	rleTimestampCountRemain int

	prevFloatValueBits   uint64
	prevLeadingZerosVal  uint32
	prevTrailingZerosVal uint32
	rleValueActive      bool
	rleValueBits        uint64
	rleValueCountRemain int

	currentDp         ts.Datapoint
	currentUnit       xtime.Unit
	currentAnnotation ts.Annotation
}

func NewM3TSZAdvancedDecoder(
	bytesPool pool.CheckedBytesPool,
	opts encoding.Options,
) encoding.Decoder {
	return &m3tszAdvancedDecoder{
		adaptiveDeltaSize:    1,
		opts:                 opts,
		bytesPool:            bytesPool,
		prevLeadingZerosVal:  ^uint32(0), // Match encoder's init
		prevTrailingZerosVal: 0,
	}
}

func (dec *m3tszAdvancedDecoder) Decode(reader xio.Reader64) (encoding.SeriesIterator, error) {
	if dec.err == encoding.ErrStreamClosed { return nil, dec.err }
	dec.Reset(reader, nil)
	return dec, dec.err
}

func (dec *m3tszAdvancedDecoder) Reset(reader xio.Reader64, _ encoding.Schema) {
	var data []byte
	if sr, ok := reader.(xio.SegmentReader); ok {
		seg := sr.Segment()
		clonedBytes := make([]byte, seg.Len())
		copy(clonedBytes, seg.Bytes)
		data = clonedBytes
	} else {
		buffer := make([]byte, 1024)
		readBytes := 0
		for {
			n, err := reader.Read(buffer[readBytes:])
			readBytes += n
			if err == io.EOF { break }
			if err != nil { dec.err = err; return }
			if len(buffer) == readBytes { /* Grow buffer */ }
		}
		data = buffer[:readBytes]
	}

	if dec.bitStream == nil { dec.bitStream = newIStream(data) } else { dec.bitStream.reset(data) }
	dec.err = nil
	if dec.opts != nil {
		dec.prevTimestamp = dec.opts.DefaultSeriesStartTime()
		dec.currentUnit = dec.opts.DefaultTimeUnit()
	} else {
		dec.prevTimestamp = time.Time{}; dec.currentUnit = xtime.Nanosecond
	}

	dec.prevDelta = 0; dec.adaptiveDeltaSize = 1
	dec.rleTimestampActive = false; dec.rleTimestampCountRemain = 0

	dec.prevFloatValueBits = 0;
	dec.prevLeadingZerosVal = ^uint32(0) // Match encoder for first val handling
	dec.prevTrailingZerosVal = 0
	dec.rleValueActive = false; dec.rleValueCountRemain = 0

	if dec.prevAnnotation != nil && dec.bytesPool != nil { /* Pool put */ }
	dec.currentDp = ts.Datapoint{}; dec.currentAnnotation = nil
}

func (dec *m3tszAdvancedDecoder) Next() bool {
	if dec.err != nil { return false }
	if dec.bitStream == nil { dec.err = encoding.ErrDecoderNotInitialized; return false }
	if dec.bitStream.remainingBits() == 0 && !dec.rleTimestampActive && !dec.rleValueActive { return false }
    // Simplified EOF check for brevity, real check is more nuanced with trailer
	if dec.bitStream.remainingBits() < 2 && !dec.rleTimestampActive && !dec.rleValueActive { // Need at least 2 bits for markers typically
		return false;
	}


	// Timestamp Decoding
	if dec.rleTimestampActive && dec.rleTimestampCountRemain > 0 {
		newTime := dec.prevTimestamp.Add(dec.rleTimestampDelta)
		dec.prevTimestamp = newTime; dec.currentDp.Timestamp = newTime
		dec.rleTimestampCountRemain--
		if dec.rleTimestampCountRemain == 0 { dec.rleTimestampActive = false }
	} else {
		if dec.rleTimestampCountRemain == 0 { dec.rleTimestampActive = false }
		if dec.bitStream.remainingBits() == 0 { return false;} // Check before decode attempt
		decodedTs, err := dec.decodeTimestampInternal()
		if err != nil {
			if err == io.EOF || err == errEndOfBlock { dec.err = nil } else { dec.err = err }
			return false
		}
		dec.currentDp.Timestamp = decodedTs
	}

	// Value Decoding
	if dec.rleValueActive && dec.rleValueCountRemain > 0 {
		dec.currentDp.Value = math.Float64frombits(dec.rleValueBits)
		dec.prevFloatValueBits = dec.rleValueBits // Update prevFloatValue for next non-RLE decode
		dec.rleValueCountRemain--
		if dec.rleValueCountRemain == 0 { dec.rleValueActive = false }
	} else {
		if dec.rleValueCountRemain == 0 { dec.rleValueActive = false }
		if dec.bitStream.remainingBits() == 0 && !(dec.rleTimestampActive && dec.rleTimestampCountRemain > 0) {
			// If timestamp also just finished RLE and no more bits, this is EOF for value too
			return false;
		}
		decodedVal, err := dec.decodeValueInternal()
		if err != nil {
			if err == io.EOF || err == errEndOfBlock { dec.err = nil } else { dec.err = err }
			// If timestamp succeeded but value failed, it's a partial point - error state.
			if dec.err != nil && (err != io.EOF && err != errEndOfBlock) { dec.currentDp.Timestamp = time.Time{} } // Invalidate partly read DP
			return false
		}
		dec.currentDp.Value = decodedVal
	}

	// dec.decodeAnnotation() // Placeholder
	return true
}

func (dec *m3tszAdvancedDecoder) decodeTimestampInternal() (time.Time, error) {
	isRLEMarker, err := dec.bitStream.readBit(); if err != nil { return time.Time{}, err }
	if isRLEMarker == one {
		if dec.prevTimestamp.IsZero() && dec.prevDelta == 0 {
			return time.Time{}, fmt.Errorf("RLE marker '1' for timestamp at start")
		}
		encodedAdditionalRepeats, err := dec.bitStream.readBits(4); if err != nil { return time.Time{}, err }
		numActualAdditionalRepeats := int(encodedAdditionalRepeats)
		newTime := dec.prevTimestamp.Add(dec.prevDelta); dec.prevTimestamp = newTime
		dec.rleTimestampActive = true; dec.rleTimestampDelta = dec.prevDelta
		dec.rleTimestampCountRemain = numActualAdditionalRepeats
		return newTime, nil
	} else {
		dec.rleTimestampActive = false; dec.rleTimestampCountRemain = 0
		adaptiveControlBit, err := dec.bitStream.readBit(); if err != nil { return time.Time{}, err }
		if adaptiveControlBit == one { return time.Time{}, fmt.Errorf("ts adaptive bit '1' unhandled") }
		if dec.adaptiveDeltaSize < 1 { return time.Time{}, fmt.Errorf("invalid ts adaptiveDeltaSize %d", dec.adaptiveDeltaSize) }
		var deltaNanos int64
		if dec.adaptiveDeltaSize == 1 {
			signBit, err := dec.bitStream.readBit(); if err != nil { return time.Time{}, err }
			if signBit == one { return time.Time{}, fmt.Errorf("ts zero delta sign bit not zero") }
			deltaNanos = 0
		} else {
			signBit, err := dec.bitStream.readBit(); if err != nil { return time.Time{}, err }
			valBits, err := dec.bitStream.readBits(dec.adaptiveDeltaSize - 1); if err != nil { return time.Time{}, err }
			deltaNanos = int64(valBits); if signBit == one { deltaNanos = -deltaNanos }
		}
		currentDelta := time.Duration(deltaNanos); dec.prevDelta = currentDelta
		newTime := dec.prevTimestamp.Add(currentDelta); dec.prevTimestamp = newTime
		requiredBits := 0
		if deltaNanos == 0 { requiredBits = 1 } else {
			absDelta := deltaNanos; if absDelta < 0 { absDelta = -absDelta }
			temp := absDelta; for temp > 0 { temp >>= 1; requiredBits++ }
			requiredBits++
		}
		if requiredBits == 0 && deltaNanos == 0 { requiredBits = 1 }
		if requiredBits > dec.adaptiveDeltaSize { dec.adaptiveDeltaSize = requiredBits }
		if dec.adaptiveDeltaSize < 1 { dec.adaptiveDeltaSize = 1 }
		return newTime, nil
	}
}

func (dec *m3tszAdvancedDecoder) decodeValueInternal() (float64, error) {
	isRLEMarker, err := dec.bitStream.readBit(); if err != nil { return 0, err }
	if isRLEMarker == one { // Value RLE
		// First value in a series cannot be RLE'd this way by current encoder
		if dec.currentDp.Timestamp == dec.opts.DefaultSeriesStartTime() && dec.prevFloatValueBits == 0 {
             return 0, fmt.Errorf("value RLE marker '1' at start of series")
        }
		encodedAdditionalRepeats, err := dec.bitStream.readBits(4); if err != nil { return 0, err }
		numActualAdditionalRepeats := int(encodedAdditionalRepeats)
		currentValue := math.Float64frombits(dec.prevFloatValueBits) // Repeated value is prev value
		dec.rleValueActive = true; dec.rleValueBits = dec.prevFloatValueBits
		dec.rleValueCountRemain = numActualAdditionalRepeats
		return currentValue, nil
	} else { // Actual float value (Gorilla)
		dec.rleValueActive = false; dec.rleValueCountRemain = 0
		diffMarker, err := dec.bitStream.readBit(); if err != nil { return 0, err }
		if diffMarker == zero { // Same as previous
			return math.Float64frombits(dec.prevFloatValueBits), nil
		}
		// Different, read control bit for LZ/TZ block info
		controlBit, err := dec.bitStream.readBit(); if err != nil { return 0, err }
		var xorVal uint64
		if controlBit == zero { // Use prev LZ/TZ
			meaningfulBits := 64 - dec.prevLeadingZerosVal - dec.prevTrailingZerosVal
			if meaningfulBits == 0 { xorVal = 0 } else if meaningfulBits == 64 {
				xorVal, err = dec.bitStream.readBits(64); if err != nil { return 0, err}
			} else if meaningfulBits < 0 || meaningfulBits > 64 {
                return 0, fmt.Errorf("invalid meaningfulBits %d from prev Val LZ/TZ", meaningfulBits)
            } else {
				significantBits, err := dec.bitStream.readBits(int(meaningfulBits)); if err != nil { return 0, err }
				xorVal = significantBits << dec.prevTrailingZerosVal
			}
		} else { // New LZ/TZ
			newLeadingZeros, err := dec.bitStream.readBits(6); if err != nil { return 0, err }
			meaningfulLenMinusOne, err := dec.bitStream.readBits(6); if err != nil { return 0, err }
			meaningfulLen := int(meaningfulLenMinusOne + 1)
			if meaningfulLen > 0 {
				significantBits, err := dec.bitStream.readBits(meaningfulLen); if err != nil { return 0, err }
				newTrailingZerosVal := 64 - uint32(newLeadingZeros) - uint32(meaningfulLen)
				if int32(newTrailingZerosVal) < 0 { return 0, fmt.Errorf("inconsistent new val LZ %d, meaningfulLen %d", newLeadingZeros, meaningfulLen)}
				xorVal = significantBits << newTrailingZerosVal
				dec.prevLeadingZerosVal = uint32(newLeadingZeros)
				dec.prevTrailingZerosVal = newTrailingZerosVal
			} else {
				xorVal = 0; dec.prevLeadingZerosVal = uint32(newLeadingZeros)
				dec.prevTrailingZerosVal = 64 - uint32(newLeadingZeros)
			}
		}
		currentValBits := dec.prevFloatValueBits ^ xorVal
		dec.prevFloatValueBits = currentValBits
		return math.Float64frombits(currentValBits), nil
	}
}

func (dec *m3tszAdvancedDecoder) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	if dec.err != nil || dec.bitStream == nil {
		return ts.Datapoint{}, dec.currentUnit, nil
	}
	return dec.currentDp, dec.currentUnit, dec.currentAnnotation
}
func (dec *m3tszAdvancedDecoder) Err() error { return dec.err }
func (dec *m3tszAdvancedDecoder) Close() { /* simplified */ dec.bitStream = nil; dec.err = encoding.ErrStreamClosed }

var errEndOfBlock = fmt.Errorf("end of block") // Custom error for trailer

func (dec *m3tszAdvancedDecoder) ID() ts.ID { return nil }
func (dec *m3tszAdvancedDecoder) Tags() ts.Tags { return ts.Tags{} }
func (dec *m3tszAdvancedDecoder) StartTime() time.Time {
	if dec.opts != nil { return dec.opts.DefaultSeriesStartTime() }
	return time.Time{}
}
func (dec *m3tszAdvancedDecoder) EndTime() time.Time { return time.Time{} }
func (dec *m3tszAdvancedDecoder) Schema() encoding.Schema {
	if dec.opts != nil { return dec.opts.Schema() }
	return nil
}
func (dec *m3tszAdvancedDecoder) ReplicaIterators() []encoding.ReaderIterator { return nil }
func (dec *m3tszAdvancedDecoder) SanityCheck() error { return nil }
func (dec *m3tszAdvancedDecoder) Stats() (encoding.IteratorStats, error) { return encoding.IteratorStats{}, nil }

// Helper (not part of struct, kept for reference if needed)
func uint64ToFloat64Bytes(u uint64) float64 {
	var f float64; buf := make([]byte, 8); binary.BigEndian.PutUint64(buf, u)
	reader := bytes.NewReader(buf); binary.Read(reader, binary.BigEndian, &f); return f
}
