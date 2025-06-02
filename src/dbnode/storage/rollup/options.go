package rollup

import (
	"errors"
	"time"
)

// Options defines the options for the rollup process.
type Options interface {
	Validate() error
	SetResolution(value time.Duration) Options
	Resolution() time.Duration
	SetNewTTL(value time.Duration) Options
	NewTTL() time.Duration
}

type options struct {
	resolution time.Duration
	newTTL     time.Duration
}

// NewOptions creates new rollup options.
func NewOptions() Options {
	return &options{}
}

func (o *options) Validate() error {
	if o.resolution <= 0 {
		return errRollupResolutionPositive
	}
	if o.newTTL <= 0 {
		return errRollupNewTTLPositive
	}
	return nil
}

func (o *options) SetResolution(value time.Duration) Options {
	opts := *o
	opts.resolution = value
	return &opts
}

func (o *options) Resolution() time.Duration {
	return o.resolution
}

func (o *options) SetNewTTL(value time.Duration) Options {
	opts := *o
	opts.newTTL = value
	return &opts
}

func (o *options) NewTTL() time.Duration {
	return o.newTTL
}

var (
	errRollupResolutionPositive = errors.New("rollup resolution must be positive")
	errRollupNewTTLPositive     = errors.New("rollup new TTL must be positive")
)
