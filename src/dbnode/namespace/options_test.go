// Copyright (c) 2017 Uber Technologies, Inc.
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

package namespace

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/retention"
)

func TestOptionsEquals(t *testing.T) {
	o1 := NewOptions()
	require.True(t, o1.Equal(o1))

	o2 := NewOptions()
	require.True(t, o1.Equal(o2))
	require.True(t, o2.Equal(o1))
}

func TestOptionsEqualsIndexOpts(t *testing.T) {
	o1 := NewOptions()
	o2 := o1.SetIndexOptions(
		o1.IndexOptions().SetBlockSize(
			o1.IndexOptions().BlockSize() * 2))
	require.True(t, o1.Equal(o1))
	require.True(t, o2.Equal(o2))
	require.False(t, o1.Equal(o2))
	require.False(t, o2.Equal(o1))
}

func TestOptionsEqualsSchema(t *testing.T) {
	o1 := NewOptions()
	s1, err := LoadSchemaHistory(testSchemaOptions)
	require.NoError(t, err)
	require.NotNil(t, s1)
	o2 := o1.SetSchemaHistory(s1)
	require.True(t, o1.Equal(o1))
	require.True(t, o2.Equal(o2))
	require.False(t, o1.Equal(o2))
	require.False(t, o2.Equal(o1))
}

func TestOptionsEqualsRetention(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	r1 := retention.NewMockOptions(ctrl)
	o1 := NewOptions().SetRetentionOptions(r1)

	r1.EXPECT().Equal(r1).Return(true)
	require.True(t, o1.Equal(o1))

	r2 := retention.NewMockOptions(ctrl)
	o2 := NewOptions().SetRetentionOptions(r2)

	r1.EXPECT().Equal(r2).Return(true)
	require.True(t, o1.Equal(o2))

	r1.EXPECT().Equal(r2).Return(false)
	require.False(t, o1.Equal(o2))

	r2.EXPECT().Equal(r1).Return(false)
	require.False(t, o2.Equal(o1))

	r2.EXPECT().Equal(r1).Return(true)
	require.True(t, o2.Equal(o1))
}

func TestOptionsValidate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rOpts := retention.NewMockOptions(ctrl)
	iOpts := NewMockIndexOptions(ctrl)
	o1 := NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(iOpts)

	iOpts.EXPECT().Enabled().Return(true).AnyTimes()

	rOpts.EXPECT().Validate().Return(nil)
	rOpts.EXPECT().RetentionPeriod().Return(time.Hour)
	rOpts.EXPECT().FutureRetentionPeriod().Return(time.Duration(0))
	rOpts.EXPECT().BlockSize().Return(time.Hour)
	iOpts.EXPECT().BlockSize().Return(time.Hour)
	require.NoError(t, o1.Validate())

	rOpts.EXPECT().Validate().Return(nil)
	rOpts.EXPECT().RetentionPeriod().Return(time.Hour)
	rOpts.EXPECT().FutureRetentionPeriod().Return(time.Duration(0))
	rOpts.EXPECT().BlockSize().Return(time.Hour)
	iOpts.EXPECT().BlockSize().Return(2 * time.Hour)
	require.Error(t, o1.Validate())

	rOpts.EXPECT().Validate().Return(fmt.Errorf("test error"))
	require.Error(t, o1.Validate())
}

func TestRollupOptionsValidate(t *testing.T) {
	// Valid options
	roValid := NewRollupOptions().SetResolution(time.Hour).SetNewTTL(24 * time.Hour)
	require.NoError(t, roValid.Validate())

	// Invalid: zero resolution
	roZeroRes := NewRollupOptions().SetResolution(0).SetNewTTL(24 * time.Hour)
	err := roZeroRes.Validate()
	require.Error(t, err)
	require.Equal(t, errRollupResolutionPositive, err)

	// Invalid: negative resolution
	roNegRes := NewRollupOptions().SetResolution(-1 * time.Hour).SetNewTTL(24 * time.Hour)
	err = roNegRes.Validate()
	require.Error(t, err)
	require.Equal(t, errRollupResolutionPositive, err)

	// Invalid: zero newTTL
	roZeroTTL := NewRollupOptions().SetResolution(time.Hour).SetNewTTL(0)
	err = roZeroTTL.Validate()
	require.Error(t, err)
	require.Equal(t, errRollupNewTTLPositive, err)

	// Invalid: negative newTTL
	roNegTTL := NewRollupOptions().SetResolution(time.Hour).SetNewTTL(-24 * time.Hour)
	err = roNegTTL.Validate()
	require.Error(t, err)
	require.Equal(t, errRollupNewTTLPositive, err)
}

func TestOptionsEqualsRollupOptions(t *testing.T) {
	// Base options
	o1 := NewOptions()

	// Options with rollup
	ro1 := NewRollupOptions().SetResolution(time.Hour).SetNewTTL(24 * time.Hour)
	o2 := o1.SetRollupOptions(ro1)

	// Options with different rollup
	ro2 := NewRollupOptions().SetResolution(2 * time.Hour).SetNewTTL(48 * time.Hour)
	o3 := o1.SetRollupOptions(ro2)

	// Options with nil rollup again (same as o1)
	o4 := o2.SetRollupOptions(nil)

	require.True(t, o1.Equal(o1)) // Self
	require.True(t, o2.Equal(o2)) // Self with rollup

	require.False(t, o1.Equal(o2)) // o1 (nil rollup) vs o2 (rollup1)
	require.False(t, o2.Equal(o1)) // o2 (rollup1) vs o1 (nil rollup)

	require.False(t, o2.Equal(o3)) // o2 (rollup1) vs o3 (rollup2)
	require.False(t, o3.Equal(o2)) // o3 (rollup2) vs o2 (rollup1)

	require.True(t, o1.Equal(o4))  // o1 (nil rollup) vs o4 (nil rollup)
	require.True(t, o4.Equal(o1))  // o4 (nil rollup) vs o1 (nil rollup)

	// Compare two different RollupOptions instances with same values
	ro1Clone := NewRollupOptions().SetResolution(time.Hour).SetNewTTL(24 * time.Hour)
	o2Clone := o1.SetRollupOptions(ro1Clone)
	require.True(t, o2.Equal(o2Clone))
	require.True(t, o2Clone.Equal(o2))
}

func TestOptionsValidateCallsRollupValidate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rOpts := retention.NewMockOptions(ctrl)
	iOpts := NewMockIndexOptions(ctrl)
	mockRollupOpts := NewMockRollupOptions(ctrl)

	// Base options, valid state for retention and index
	rOpts.EXPECT().Validate().Return(nil).AnyTimes()
	iOpts.EXPECT().Enabled().Return(false).AnyTimes() // Simplest path for main options validate

	// Case 1: No rollup options, should be valid
	opts1 := NewOptions().SetRetentionOptions(rOpts).SetIndexOptions(iOpts)
	require.NoError(t, opts1.Validate())

	// Case 2: Valid rollup options
	opts2 := opts1.SetRollupOptions(mockRollupOpts)
	mockRollupOpts.EXPECT().Validate().Return(nil)
	require.NoError(t, opts2.Validate())

	// Case 3: Invalid rollup options
	opts3 := opts1.SetRollupOptions(mockRollupOpts)
	expectedErr := fmt.Errorf("rollup validation error")
	mockRollupOpts.EXPECT().Validate().Return(expectedErr)
	err := opts3.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid rollup options: rollup validation error")
}

func TestOptionsValidateWithExtendedOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	extendedOpts := NewMockExtendedOptions(ctrl)
	opts := NewOptions().SetExtendedOptions(extendedOpts)

	extendedOpts.EXPECT().Validate().Return(nil)
	require.NoError(t, opts.Validate())

	extendedOpts.EXPECT().Validate().Return(fmt.Errorf("test error"))
	require.Error(t, opts.Validate())
}

func TestOptionsValidateBlockSizeMustBeMultiple(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rOpts := retention.NewMockOptions(ctrl)
	iOpts := NewMockIndexOptions(ctrl)
	o1 := NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(iOpts)

	iOpts.EXPECT().Enabled().Return(true).AnyTimes()

	rOpts.EXPECT().Validate().Return(nil)
	rOpts.EXPECT().RetentionPeriod().Return(4 * time.Hour).AnyTimes()
	rOpts.EXPECT().FutureRetentionPeriod().Return(time.Duration(0)).AnyTimes()
	rOpts.EXPECT().BlockSize().Return(2 * time.Hour).AnyTimes()
	iOpts.EXPECT().BlockSize().Return(3 * time.Hour).AnyTimes()
	require.Error(t, o1.Validate())
}

func TestOptionsValidateBlockSizePositive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rOpts := retention.NewMockOptions(ctrl)
	iOpts := NewMockIndexOptions(ctrl)
	o1 := NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(iOpts)

	iOpts.EXPECT().Enabled().Return(true).AnyTimes()

	rOpts.EXPECT().Validate().Return(nil)
	rOpts.EXPECT().RetentionPeriod().Return(4 * time.Hour).AnyTimes()
	rOpts.EXPECT().FutureRetentionPeriod().Return(time.Duration(0)).AnyTimes()
	rOpts.EXPECT().BlockSize().Return(2 * time.Hour).AnyTimes()
	iOpts.EXPECT().BlockSize().Return(0 * time.Hour).AnyTimes()
	require.Error(t, o1.Validate())

	rOpts.EXPECT().Validate().Return(nil)
	rOpts.EXPECT().RetentionPeriod().Return(4 * time.Hour).AnyTimes()
	rOpts.EXPECT().BlockSize().Return(2 * time.Hour).AnyTimes()
	iOpts.EXPECT().BlockSize().Return(-2 * time.Hour).AnyTimes()
	require.Error(t, o1.Validate())
}

func TestOptionsValidateNoIndexing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rOpts := retention.NewMockOptions(ctrl)
	iOpts := NewMockIndexOptions(ctrl)
	o1 := NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(iOpts)

	iOpts.EXPECT().Enabled().Return(false).AnyTimes()

	rOpts.EXPECT().Validate().Return(nil)
	require.NoError(t, o1.Validate())
}

func TestOptionsValidateStagingStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rOpts := retention.NewMockOptions(ctrl)
	iOpts := NewMockIndexOptions(ctrl)
	o1 := NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(iOpts)

	iOpts.EXPECT().Enabled().Return(true).AnyTimes()

	rOpts.EXPECT().Validate().Return(nil).AnyTimes()
	rOpts.EXPECT().RetentionPeriod().Return(time.Hour).AnyTimes()
	rOpts.EXPECT().FutureRetentionPeriod().Return(time.Duration(0)).AnyTimes()
	rOpts.EXPECT().BlockSize().Return(time.Hour).AnyTimes()
	iOpts.EXPECT().BlockSize().Return(time.Hour).AnyTimes()
	require.NoError(t, o1.Validate())

	o1 = o1.SetStagingState(StagingState{status: StagingStatus(12)})
	require.Error(t, o1.Validate())
}
