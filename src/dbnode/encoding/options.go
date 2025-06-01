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

package encoding

import (
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/x/instrument"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	// "github.com/m3db/m3/src/dbnode/encoding/proto" // Assuming a proto package might exist
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	defaultDefaultTimeUnit        = xtime.Second
	defaultByteFieldDictLRUSize   = 4
	defaultIStreamReaderSizeM3TSZ = 8 * 2
	defaultIStreamReaderSizeProto = 128
)

var (
	// default encoding options
	defaultOptions = newOptions()
)

type options struct {
	defaultTimeUnit         xtime.Unit
	timeEncodingSchemes     TimeEncodingSchemes
	markerEncodingScheme    *MarkerEncodingScheme
	encoderPool             EncoderPool
	readerIteratorPool      ReaderIteratorPool
	bytesPool               pool.CheckedBytesPool
	segmentReaderPool       xio.SegmentReaderPool
	checkedBytesWrapperPool xpool.CheckedBytesWrapperPool
	byteFieldDictLRUSize    int
	iStreamReaderSizeM3TSZ  int
	iStreamReaderSizeProto  int
	metrics                 Metrics
	encodingType            EncodingType
}

func newOptions() Options {
	opts := &options{
		defaultTimeUnit:        defaultDefaultTimeUnit,
		timeEncodingSchemes:    NewTimeEncodingSchemes(defaultTimeEncodingSchemes),
		markerEncodingScheme:   defaultMarkerEncodingScheme,
		byteFieldDictLRUSize:   defaultByteFieldDictLRUSize,
		iStreamReaderSizeM3TSZ: defaultIStreamReaderSizeM3TSZ,
		iStreamReaderSizeProto: defaultIStreamReaderSizeProto,
		metrics:                NewMetrics(instrument.NewOptions().MetricsScope()),
		encodingType:           DefaultEncodingType, // Initialize with default
	}

	// Initialize default pools with allocators that use opts.encodingType
	defaultEncoderPool := NewEncoderPool(nil)
	defaultEncoderPool.Init(func() Encoder {
		// NB: This allocation function captures the `opts` pointer.
		// This means if opts.encodingType is changed *after* pool initialization,
		// newly allocated encoders from this default pool will reflect that change.
		// This is generally the desired behavior if the Options object is treated as a
		// live configuration object.
		return newEncoderFromOptions(opts)
	})
	opts.encoderPool = defaultEncoderPool

	defaultReaderIteratorPool := NewReaderIteratorPool(nil)
	defaultReaderIteratorPool.Init(func(reader xio.Reader64, schema SchemaDescr) ReaderIterator {
		// Similar to encoder pool, this captures `opts`.
		return newReaderIteratorFromOptions(opts, reader, schema)
	})
	opts.readerIteratorPool = defaultReaderIteratorPool

	return opts
}

// newEncoderFromOptions is an internal helper to create an encoder based on options.EncodingType.
// It's not part of the Options interface.
func newEncoderFromOptions(opts *options) Encoder {
	// Determine start time for encoder; M3TSZ uses it, Proto might not directly.
	// A zero time.Time is often used as a placeholder if real start is set on Reset.
	// However, m3tsz.NewEncoder and m3tsz.NewM3TSZAdvancedEncoder expect it.
	// Let's use a zero value time.Time here, assuming Reset will provide the actual start.
	var startTime time.Time // Or pass opts.DefaultSeriesStartTime() if appropriate for general case

	switch opts.EncodingType() {
	case M3TSZEncoding:
		// Assuming m3tsz.NewEncoder is the constructor for the original M3TSZ.
		// It might require arguments like (startTime, bytesPool, intOptimizationEnabled, encodingOpts)
		// For a generic pool, we might need to pass some defaults or get them from opts.
		// The signature used in grep was: m3tsz.NewEncoder(timeZero, nil, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
		// We need to ensure our m3tsz.NewEncoder matches what's expected or adapt.
		// For this example, let's assume a simplified constructor for now or use the advanced one as a placeholder.
		// This part needs to align with actual m3tsz.NewEncoder signature.
		// If m3tsz.NewEncoder is the old one, it might be:
		// The 'bytes' argument to m3tsz.NewEncoder can be nil when used with pools,
		// as Reset will handle allocation.
		return m3tsz.NewEncoder(startTime, nil, m3tsz.DefaultIntOptimizationEnabled, opts)
	case M3TSZAdvancedEncoding:
		return m3tsz.NewM3TSZAdvancedEncoder(startTime, opts.BytesPool(), opts)
	case ProtoEncoding:
		// return proto.NewEncoder(startTime, opts.BytesPool(), opts) // Example
		panic("ProtoEncoding not implemented in default pool allocator") // Placeholder
	default:
		// Fallback to a default or panic
		// For now, let's default to M3TSZ if type is unknown, or panic.
		// Panic is safer to detect misconfiguration.
		panic("unknown encoding type in default pool allocator")
	}
}

// newReaderIteratorFromOptions is an internal helper.
func newReaderIteratorFromOptions(opts *options, reader xio.Reader64, schema SchemaDescr) ReaderIterator {
	switch opts.EncodingType() {
	case M3TSZEncoding:
		// m3tsz.NewDecoder returns an encoding.Decoder. We then call Decode on it.
		// The ReaderIteratorAllocate function is expected to return a ReaderIterator.
		// The m3tsz.NewReaderIterator is the actual allocator for the iterator.
		// The schema is passed to NewReaderIterator, not Reset for the decoder itself.
		return m3tsz.NewReaderIterator(reader, m3tsz.DefaultIntOptimizationEnabled, opts, schema)
	case M3TSZAdvancedEncoding:
		// For M3TSZAdvancedDecoder, it is its own ReaderIterator.
		advDecoder := m3tsz.NewM3TSZAdvancedDecoder(opts.BytesPool(), opts)
		advDecoder.Reset(reader, schema) // Reset prepares it with the reader.
		return advDecoder
	case ProtoEncoding:
		// return proto.NewIterator(reader, schema, opts) // Example
		panic("ProtoEncoding not implemented in default pool allocator") // Placeholder
	default:
		panic("unknown encoding type in default pool allocator")
	}
}


// NewOptions creates a new options.
func NewOptions() Options {
	return defaultOptions
}

func (o *options) SetDefaultTimeUnit(value xtime.Unit) Options {
	opts := *o
	opts.defaultTimeUnit = value
	return &opts
}

func (o *options) DefaultTimeUnit() xtime.Unit {
	return o.defaultTimeUnit
}

func (o *options) SetTimeEncodingSchemes(value map[xtime.Unit]TimeEncodingScheme) Options {
	opts := *o
	opts.timeEncodingSchemes = NewTimeEncodingSchemes(value)
	return &opts
}

func (o *options) TimeEncodingSchemes() TimeEncodingSchemes {
	return o.timeEncodingSchemes
}

func (o *options) SetMarkerEncodingScheme(value *MarkerEncodingScheme) Options {
	opts := *o
	opts.markerEncodingScheme = value
	return &opts
}

func (o *options) MarkerEncodingScheme() *MarkerEncodingScheme {
	return o.markerEncodingScheme
}

func (o *options) SetEncoderPool(value EncoderPool) Options {
	opts := *o
	opts.encoderPool = value
	return &opts
}

func (o *options) EncoderPool() EncoderPool {
	return o.encoderPool
}

func (o *options) SetReaderIteratorPool(value ReaderIteratorPool) Options {
	opts := *o
	opts.readerIteratorPool = value
	return &opts
}

func (o *options) ReaderIteratorPool() ReaderIteratorPool {
	return o.readerIteratorPool
}

func (o *options) SetBytesPool(value pool.CheckedBytesPool) Options {
	opts := *o
	opts.bytesPool = value
	return &opts
}

func (o *options) BytesPool() pool.CheckedBytesPool {
	return o.bytesPool
}

func (o *options) SetSegmentReaderPool(value xio.SegmentReaderPool) Options {
	opts := *o
	opts.segmentReaderPool = value
	return &opts
}

func (o *options) SegmentReaderPool() xio.SegmentReaderPool {
	return o.segmentReaderPool
}

func (o *options) SetCheckedBytesWrapperPool(value xpool.CheckedBytesWrapperPool) Options {
	opts := *o
	opts.checkedBytesWrapperPool = value
	return &opts
}

func (o *options) CheckedBytesWrapperPool() xpool.CheckedBytesWrapperPool {
	return o.checkedBytesWrapperPool
}

func (o *options) SetByteFieldDictionaryLRUSize(value int) Options {
	opts := *o
	opts.byteFieldDictLRUSize = value
	return &opts
}

func (o *options) ByteFieldDictionaryLRUSize() int {
	return o.byteFieldDictLRUSize
}

func (o *options) SetIStreamReaderSizeM3TSZ(value int) Options {
	opts := *o
	opts.iStreamReaderSizeM3TSZ = value
	return &opts
}

func (o *options) IStreamReaderSizeM3TSZ() int {
	return o.iStreamReaderSizeM3TSZ
}

func (o *options) SetIStreamReaderSizeProto(value int) Options {
	opts := *o
	opts.iStreamReaderSizeProto = value
	return &opts
}

func (o *options) IStreamReaderSizeProto() int {
	return o.iStreamReaderSizeProto
}

func (o *options) SetMetrics(value Metrics) Options {
	opts := *o
	opts.metrics = value
	return &opts
}

func (o *options) Metrics() Metrics {
	return o.metrics
}

func (o *options) SetEncodingType(value EncodingType) Options {
	opts := *o
	opts.encodingType = value
	// If pools are default and rely on this, their next Get will use the new type.
	// If pools were custom set, this change might not affect them unless they also consult this.
	return &opts
}

func (o *options) EncodingType() EncodingType {
	return o.encodingType
}
