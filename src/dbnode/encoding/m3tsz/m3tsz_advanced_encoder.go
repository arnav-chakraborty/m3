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
	"math"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

type m3tszAdvancedEncoder struct {
	bitStream         *ostream
	prevTimestamp     time.Time
	prevDelta         time.Duration
	adaptiveDeltaSize int // Stores the current size of the delta representation in bits
	rleTimestampCount int // Counter for RLE of timestamp deltas

	// State for float value encoding (Gorilla-style)
	prevFloatValueBits uint64
	prevLeadingZeros   uint32 // Renamed from leadingZeros for clarity
	prevTrailingZeros  uint32 // Renamed from trailingZeros for clarity
	rleValueCount      int    // Counter for RLE of float values
}

// NewM3TSZAdvancedEncoder creates a new M3TSZ-Advanced encoder.
func NewM3TSZAdvancedEncoder(
	start time.Time,
	bytesPool pool.CheckedBytesPool,
	opts encoding.Options,
) encoding.Encoder {
	encoder := &m3tszAdvancedEncoder{
		prevTimestamp:     start,
		adaptiveDeltaSize: 1, // Start with a small delta size
		// Initialize prevLeadingZeros to a value that ensures the first XORed value stores its own LZ/TZ.
		// Gorilla paper suggests first value is stored uncompressed, or use specific initial values.
		// Using ^uint32(0) ensures the first comparison `currLZ >= enc.prevLZ && currTZ >= enc.prevTZ` fails.
		prevLeadingZeros:  ^uint32(0),
		prevTrailingZeros: 0,
	}
	if opts != nil && opts.EncoderPool() != nil {
		encoder.bitStream = newOStream(bytesPool.Get(nil), true, opts.EncoderPool())
	} else {
		encoder.bitStream = newOStream(bytesPool.Get(nil), true, nil)
	}
	return encoder
}

func (enc *m3tszAdvancedEncoder) Encode(dp ts.Datapoint, unit xtime.Unit, annotation ts.Annotation) error {
	if enc.bitStream == nil {
		return encoding.ErrEncoderClosed
	}

	enc.encodeTimestamp(dp.Timestamp, false)
	enc.encodeValue(dp.Value, false)
	// enc.encodeAnnotation(annotation) // Placeholder
	return nil
}

func (enc *m3tszAdvancedEncoder) encodeTimestamp(timestamp time.Time, flushCurrent bool) {
	var delta time.Duration
	if !flushCurrent {
		delta = timestamp.Sub(enc.prevTimestamp)
	}

	if !flushCurrent && enc.rleTimestampCount > 0 && delta == enc.prevDelta {
		enc.rleTimestampCount++
	} else {
		if enc.rleTimestampCount > 1 {
			enc.writeTimestampDeltaRun()
		}
		if !flushCurrent {
			enc.writeActualTimestampDelta(delta)
			enc.prevDelta = delta
			enc.prevTimestamp = timestamp
			enc.rleTimestampCount = 1
		} else {
			if enc.rleTimestampCount == 1 {
				enc.rleTimestampCount = 0
			}
		}
	}
}

func (enc *m3tszAdvancedEncoder) writeActualTimestampDelta(delta time.Duration) {
	enc.bitStream.writeBit(zero) // Marker for non-RLE delta
	deltaNanos := delta.Nanoseconds()
	requiredBits := 0
	if deltaNanos == 0 {
		requiredBits = 1
	} else {
		absDelta := deltaNanos; if absDelta < 0 { absDelta = -absDelta }
		temp := absDelta; for temp > 0 { temp >>= 1; requiredBits++ }
		requiredBits++ // Sign bit
	}
	if requiredBits == 0 && deltaNanos == 0 { requiredBits = 1; }
	if requiredBits > enc.adaptiveDeltaSize { enc.adaptiveDeltaSize = requiredBits; }
	if enc.adaptiveDeltaSize < 1 { enc.adaptiveDeltaSize = 1; }

	enc.bitStream.writeBit(zero) // Control bit for adaptive size
	if enc.adaptiveDeltaSize == 1 {
		enc.bitStream.writeBit(zero) // Sign bit for 0
	} else {
		if deltaNanos >= 0 {
			enc.bitStream.writeBit(zero); enc.bitStream.writeBits(uint64(deltaNanos), enc.adaptiveDeltaSize-1)
		} else {
			enc.bitStream.writeBit(one); enc.bitStream.writeBits(uint64(-deltaNanos), enc.adaptiveDeltaSize-1)
		}
	}
}

func (enc *m3tszAdvancedEncoder) writeTimestampDeltaRun() {
	enc.bitStream.writeBit(one) // RLE Marker
	additionalRepeats := enc.rleTimestampCount - 1
	if additionalRepeats < 1 { additionalRepeats = 1; }
	if additionalRepeats > 15 { additionalRepeats = 15; } // Cap run
	enc.bitStream.writeBits(uint64(additionalRepeats), 4)
	enc.rleTimestampCount = 0
}

func (enc *m3tszAdvancedEncoder) encodeValue(value float64, flushCurrent bool) {
	currentValueBits := math.Float64bits(value)

	if !flushCurrent && enc.rleValueCount > 0 && currentValueBits == enc.prevFloatValueBits {
		enc.rleValueCount++
	} else {
		if enc.rleValueCount > 1 {
			enc.writeValueRLE()
		}
		if !flushCurrent {
			enc.writeActualFloatValue(currentValueBits)
			enc.prevFloatValueBits = currentValueBits
			enc.rleValueCount = 1
		} else {
			if enc.rleValueCount == 1 {
				enc.rleValueCount = 0
			}
		}
	}
}

func (enc *m3tszAdvancedEncoder) writeActualFloatValue(currentValueBits uint64) {
	enc.bitStream.writeBit(zero) // Marker for non-RLE float value

	xorVal := currentValueBits ^ enc.prevFloatValueBits
	if xorVal == 0 {
		enc.bitStream.writeBit(zero) // Value is same as previous
		return
	}
	enc.bitStream.writeBit(one) // Value is different

	currLeadingZeros := uint32(math.LeadingZeros64(xorVal))
	currTrailingZeros := uint32(math.TrailingZeros64(xorVal))

	if currLeadingZeros >= enc.prevLeadingZeros && currTrailingZeros >= enc.prevTrailingZeros {
		enc.bitStream.writeBit(zero) // Use stored block info
		meaningfulBits := 64 - enc.prevLeadingZeros - enc.prevTrailingZeros
		if meaningfulBits > 0 && meaningfulBits < 64 { // Avoid writing if 0 or full 64
			enc.bitStream.writeBits(xorVal>>enc.prevTrailingZeros, int(meaningfulBits))
		} else if meaningfulBits == 64 { // All bits are significant
			enc.bitStream.writeBits(xorVal, 64)
		}
		// if meaningfulBits is 0, xorVal must be 0, which is handled by the first check.
	} else {
		enc.bitStream.writeBit(one) // Store new block info
		enc.bitStream.writeBits(uint64(currLeadingZeros), 6) // 6 bits for leading zeros (0-63)

		meaningfulBitsLen := 64 - currLeadingZeros - currTrailingZeros
		// Store (length - 1) in 6 bits (0-63 for lengths 1-64)
		if meaningfulBitsLen == 0 { // Should not happen if xorVal != 0
			enc.bitStream.writeBits(0, 6) // Represents length 1, but no bits to write effectively
		} else {
			enc.bitStream.writeBits(uint64(meaningfulBitsLen-1), 6)
			enc.bitStream.writeBits(xorVal>>currTrailingZeros, int(meaningfulBitsLen))
		}
		enc.prevLeadingZeros = currLeadingZeros
		enc.prevTrailingZeros = currTrailingZeros
	}
}

func (enc *m3tszAdvancedEncoder) writeValueRLE() {
	enc.bitStream.writeBit(one) // RLE Marker for values
	additionalRepeats := enc.rleValueCount - 1
	if additionalRepeats < 1 { additionalRepeats = 1; }
	if additionalRepeats > 15 { additionalRepeats = 15; } // Cap run
	enc.bitStream.writeBits(uint64(additionalRepeats), 4)
	enc.rleValueCount = 0
}

func (enc *m3tszAdvancedEncoder) Stream(blockReader encoding.BlockReader) (encoding.Segment, error) {
	if enc.bitStream == nil { return ts.Segment{}, encoding.ErrEncoderClosed; }
	enc.encodeTimestamp(time.Time{}, true) // Flush timestamp RLE
	enc.encodeValue(0, true)               // Flush value RLE

	numUnusedBits := uint8(enc.bitStream.getUnusedBitsInCurrentByte())
	var blockTrailer byte
	blockTrailer |= 1 << 7; blockTrailer &^= 1 << 6
	blockTrailer |= numUnusedBits & 0x3F
	enc.bitStream.writeByte(blockTrailer)

	segment := ts.NewSegment(enc.bitStream.bytes(), nil, ts.FinalizeHead)
	enc.bitStream = nil // Encoder is now closed
	return segment, nil
}

func (enc *m3tszAdvancedEncoder) Reset(start time.Time, capacity int, opts encoding.Options) {
	enc.prevTimestamp = start
	enc.prevDelta = 0
	enc.adaptiveDeltaSize = 1
	enc.rleTimestampCount = 0

	enc.prevFloatValueBits = 0
	enc.prevLeadingZeros = ^uint32(0) // Ensures first value stores its LZ/TZ
	enc.prevTrailingZeros = 0
	enc.rleValueCount = 0

	if enc.bitStream == nil {
		var initialBytes []byte; var pooler encoding.EncoderPool
		if opts != nil {
			pooler = opts.EncoderPool()
			if pooler != nil && pooler.BytesPool() != nil {
				initialBytes = pooler.BytesPool().Get(nil)
			}
		}
		enc.bitStream = newOStream(initialBytes, true, pooler)
	}
	enc.bitStream.reset(enc.bitStream.rawBuffer()[:0])
}

func (enc *m3tszAdvancedEncoder) Close() {
	if enc.bitStream != nil {
		enc.bitStream.close(); enc.bitStream = nil
	}
}

func (enc *m3tszAdvancedEncoder) encodeAnnotation(annotation ts.Annotation) { /* Placeholder */ }
func (enc *m3tszAdvancedEncoder) LastEncodedTime() (time.Time, error) {
	if enc.bitStream == nil { return time.Time{}, encoding.ErrEncoderClosed }
	return enc.prevTimestamp, nil
}
func (enc *m3tszAdvancedEncoder) Len() int {
	if enc.bitStream == nil { return 0 }
	return enc.bitStream.len()
}
func (enc *m3tszAdvancedEncoder) Discard() { if enc.bitStream != nil { enc.bitStream.discard() } }
func (enc *m3tszAdvancedEncoder) Bytes() []byte {
	if enc.bitStream == nil { return nil }
	return enc.bitStream.bytes()
}

// Helper (not part of struct, kept for reference if needed, but math package is better)
func float64ToUint64Bytes(f float64) uint64 {
	var v uint64
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, f)
	v = binary.BigEndian.Uint64(buf.Bytes())
	return v
}
func countLeadingZerosSlow(val uint64) uint32 {
	if val == 0 { return 64 }
	lz := 0; for i := 63; i >= 0; i-- { if (val>>i)&1 == 0 { lz++ } else { break } }; return uint32(lz)
}
func countTrailingZerosSlow(val uint64) uint32 {
	if val == 0 { return 64 }
	tz := 0; for i := 0; i < 64; i++ { if (val>>i)&1 == 0 { tz++ } else { break } }; return uint32(tz)
}
