package rollup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRollupOptionsValidate(t *testing.T) {
	// Valid options
	roValid := NewOptions().SetResolution(time.Hour).SetNewTTL(24 * time.Hour)
	require.NoError(t, roValid.Validate())

	// Invalid: zero resolution
	roZeroRes := NewOptions().SetResolution(0).SetNewTTL(24 * time.Hour)
	err := roZeroRes.Validate()
	require.Error(t, err)
	require.Equal(t, errRollupResolutionPositive, err) // Assumes errRollupResolutionPositive is defined in rollup/options.go

	// Invalid: negative resolution
	roNegRes := NewOptions().SetResolution(-1 * time.Hour).SetNewTTL(24 * time.Hour)
	err = roNegRes.Validate()
	require.Error(t, err)
	require.Equal(t, errRollupResolutionPositive, err)

	// Invalid: zero newTTL
	roZeroTTL := NewOptions().SetResolution(time.Hour).SetNewTTL(0)
	err = roZeroTTL.Validate()
	require.Error(t, err)
	require.Equal(t, errRollupNewTTLPositive, err) // Assumes errRollupNewTTLPositive is defined in rollup/options.go

	// Invalid: negative newTTL
	roNegTTL := NewOptions().SetResolution(time.Hour).SetNewTTL(-24 * time.Hour)
	err = roNegTTL.Validate()
	require.Error(t, err)
	require.Equal(t, errRollupNewTTLPositive, err)
}

func TestRollupOptionsSettersAndGetters(t *testing.T) {
	opts := NewOptions()

	res := 10 * time.Minute
	ttl := 240 * time.Hour

	opts.SetResolution(res)
	require.Equal(t, res, opts.Resolution())

	opts.SetNewTTL(ttl)
	require.Equal(t, ttl, opts.NewTTL())

	// Test immutability of Setters
	opts2 := opts.SetResolution(res * 2)
	require.Equal(t, res, opts.Resolution(), "Original options should not be modified")
	require.Equal(t, res*2, opts2.Resolution(), "New options should have the new value")

	opts3 := opts.SetNewTTL(ttl * 2)
	require.Equal(t, ttl, opts.NewTTL(), "Original options should not be modified")
	require.Equal(t, ttl*2, opts3.NewTTL(), "New options should have the new value")
}
