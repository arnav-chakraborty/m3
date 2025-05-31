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

package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Mock FilesystemOptions for testing
type mockFsOpts struct {
	filePathPrefix string
}

func (m *mockFsOpts) FilePathPrefix() string { return m.filePathPrefix }
func (m *mockFsOpts) InfoReaderBufferSize() int { return 0 }
func (m *mockFsOpts) DataReaderBufferSize() int { return 0 }
func (m *mockFsOpts) WriterBufferSize() int   { return 0 }

// newTestRollupRuleOptions creates a retention.RollupRuleOptions for testing.
func newTestRollupRuleOptions(ctrl *gomock.Controller, resolution time.Duration, age time.Duration) retention.RollupRuleOptions {
	r := retention.NewMockRollupRuleOptions(ctrl)
	r.EXPECT().Resolution().Return(resolution).AnyTimes()
	r.EXPECT().Age().Return(age).AnyTimes()
	// For marker file naming consistency
	r.EXPECT().Resolution().Return(resolution).AnyTimes()
	return r
}

func TestRollupProcessor_NeedsRollup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir, err := os.MkdirTemp("", "rollup_test_needs_rollup")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	mockNsMetadata := namespace.NewMockMetadata(ctrl)
	mockNsMetadata.EXPECT().ID().Return(ident.StringID("testns")).AnyTimes()

	now := xtime.Now()
	oneHourAgo := now.Add(-time.Hour)
	threeHoursAgo := now.Add(-3 * time.Hour)

	fsOpts := &mockFsOpts{filePathPrefix: tempDir}

	clockOpts := clock.NewOptions().SetNowFn(func() time.Time { return now.ToTime() })

	logger, _ := zap.NewDevelopment()

	rpOpts := rollupProcessorOptions{
		clockOpts: clockOpts,
		fsOpts:    fsOpts,
		logger:    logger,
		// blockRetriever, pools are not used by NeedsRollup
	}
	processor, err := NewRollupProcessor(rpOpts)
	require.NoError(t, err)
	rp := processor.(*rollupProcessor) // Cast to access markerFilePath directly for test setup

	tests := []struct {
		name            string
		blockTime       xtime.UnixNano
		ruleAge         time.Duration
		setupMarkerFile bool
		expected        bool
		expectedErr     bool
	}{
		{
			name:            "block is too new",
			blockTime:       oneHourAgo,
			ruleAge:         2 * time.Hour,
			setupMarkerFile: false,
			expected:        false,
			expectedErr:     false,
		},
		{
			name:            "block old enough, no marker",
			blockTime:       threeHoursAgo,
			ruleAge:         2 * time.Hour,
			setupMarkerFile: false,
			expected:        true,
			expectedErr:     false,
		},
		{
			name:            "block old enough, marker exists",
			blockTime:       threeHoursAgo,
			ruleAge:         2 * time.Hour,
			setupMarkerFile: true,
			expected:        false,
			expectedErr:     false,
		},
		{
			name:            "block exactly at rule age, no marker",
			blockTime:       now.Add(-2 * time.Hour),
			ruleAge:         2 * time.Hour,
			setupMarkerFile: false,
			expected:        true, // Age is >= rule.Age()
			expectedErr:     false,
		},
		{
			name:            "block just past rule age, no marker",
			blockTime:       now.Add(-2 * time.Hour).Add(-time.Second),
			ruleAge:         2 * time.Hour,
			setupMarkerFile: false,
			expected:        true,
			expectedErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := newTestRollupRuleOptions(ctrl, 1*time.Minute, tt.ruleAge) // Resolution doesn't matter for NeedsRollup logic itself

			markerPath := rp.markerFilePath(mockNsMetadata, 0, tt.blockTime, rule)

			if tt.setupMarkerFile {
				err := os.MkdirAll(filepath.Dir(markerPath), os.ModePerm)
				require.NoError(t, err)
				f, err := os.Create(markerPath)
				require.NoError(t, err)
				f.Close()
			} else {
				// Ensure no marker from previous test run
				os.Remove(markerPath)
			}

			got, err := processor.NeedsRollup(mockNsMetadata, 0, tt.blockTime, rule)

			if tt.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, got)
			}

			// Clean up marker file for next test if it was created
			if tt.setupMarkerFile {
				os.Remove(markerPath)
			}
		})
	}
}

// TestRollupProcessor_Process_LogicOnly tests the control flow of Process
// without actual data aggregation.
func TestRollupProcessor_Process_LogicOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir, err := os.MkdirTemp("", "rollup_test_process_logic")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	mockNsID := ident.StringID("testns_process_logic")
	mockNsOpts := namespace.NewMockOptions(ctrl)
	mockNsMetadata := namespace.NewMockMetadata(ctrl)
	mockNsMetadata.EXPECT().ID().Return(mockNsID).AnyTimes()
	mockNsMetadata.EXPECT().Options().Return(mockNsOpts).AnyTimes()

	now := xtime.Now()
	blockTime := now.Add(-3 * time.Hour) // Old enough to trigger rollup for a rule with age <= 3h

	fsOpts := &mockFsOpts{filePathPrefix: tempDir}
	clockOpts := clock.NewOptions().SetNowFn(func() time.Time { return now.ToTime() })
	logger, _ := zap.NewDevelopment()

	// Mock pools (not strictly used in this logic-only test if data processing is stubbed)
	mockIdentPool := ident.NewMockPool(ctrl)
	mockEncoderPool := encoding.NewMockEncoderPool(ctrl)
	mockIterPool := encoding.NewMockMultiReaderIteratorPool(ctrl)

	// Mock persist fs.Options for processRule's NewReader/NewWriter (even if conceptual)
	// For this logic test, these won't be deeply interacted with if processRule's core data part is stubbed.
	// However, NewRollupProcessor expects a non-nil fsOpts.
	// The fsOpts above is our mockFsOpts, which is fine for markerFilePath.
	// If processRule internally uses methods from pfs.Options that mockFsOpts doesn't have,
	// we'd need a more complete mock or use a real pfs.Options with tempDir.
	// Let's assume the current mockFsOpts is enough for path generation.
	// For NewReader/NewWriter, processRule uses p.fs which is pfs.NewFileSystem(p.opts.fsOpts).
	// So p.opts.fsOpts needs to be a real pfs.Options if we were to test actual file ops.
	// For logic-only, we assume processRule's data part is skipped or returns no error.

	rpOpts := rollupProcessorOptions{
		clockOpts:           clockOpts,
		fsOpts:              fsOpts, // This is our simplified mock
		logger:              logger,
		identifierPool:      mockIdentPool,
		encoderPool:         mockEncoderPool,
		multiReaderIterPool: mockIterPool,
		// iteratorPools, seriesPool not strictly needed if data processing is fully stubbed
	}

	// Create a real RollupProcessor, but we'll control its behavior via NeedsRollup mostly.
	// The challenge is that processRule calls aggregateSeries and writer parts internally.
	// For a "logic-only" test, we'd ideally mock those internal steps.
	// Since we can't easily mock private methods of rollupProcessor,
	// we test the observable side effect: marker file creation based on NeedsRollup.
	// The current Process method's data handling part is placeholder, so this works.
	// If data handling returned specific errors, we'd need more complex setup.

	processor, err := NewRollupProcessor(rpOpts)
	require.NoError(t, err)
	rp := processor.(*rollupProcessor)

	rule1Age := 2 * time.Hour
	rule1Res := 5 * time.Minute
	mockRule1 := newTestRollupRuleOptions(ctrl, rule1Res, rule1Age)

	rule2Age := 4 * time.Hour // This rule should not be triggered by blockTime
	rule2Res := 10 * time.Minute
	mockRule2 := newTestRollupRuleOptions(ctrl, rule2Res, rule2Age)

	rules := []retention.RollupRuleOptions{mockRule1, mockRule2}

	mockRetentionOpts := retention.NewMockOptions(ctrl)
	mockRetentionOpts.EXPECT().RollupRules().Return(rules).AnyTimes()
	mockNsOpts.EXPECT().RetentionOptions().Return(mockRetentionOpts).AnyTimes()

	// --- Test Case 1: Rule1 needs rollup, Rule2 does not ---
	t.Run("Rule1_NeedsRollup_Rule2_NoRollup", func(t *testing.T) {
		// Ensure no markers from previous runs
		os.Remove(rp.markerFilePath(mockNsMetadata, 0, blockTime, mockRule1))
		os.Remove(rp.markerFilePath(mockNsMetadata, 0, blockTime, mockRule2))

		// Call Process. Since data processing is placeholder, it should proceed to marker.
		err := processor.Process(context.NewBackground(), mockNsMetadata, 0, blockTime)
		require.NoError(t, err) // Assuming placeholder data ops don't error

		// Check marker for Rule1 (should exist)
		_, statErr1 := os.Stat(rp.markerFilePath(mockNsMetadata, 0, blockTime, mockRule1))
		require.NoError(t, statErr1, "marker file for rule1 should be created")

		// Check marker for Rule2 (should NOT exist)
		_, statErr2 := os.Stat(rp.markerFilePath(mockNsMetadata, 0, blockTime, mockRule2))
		require.True(t, os.IsNotExist(statErr2), "marker file for rule2 should NOT be created")

		// Cleanup markers
		os.Remove(rp.markerFilePath(mockNsMetadata, 0, blockTime, mockRule1))
	})

	// --- Test Case 2: Marker for Rule1 already exists ---
	t.Run("Rule1_MarkerExists", func(t *testing.T) {
		// Setup: Marker for Rule1 exists
		markerPath1 := rp.markerFilePath(mockNsMetadata, 0, blockTime, mockRule1)
		err := os.MkdirAll(filepath.Dir(markerPath1), os.ModePerm)
		require.NoError(t, err)
		f, err := os.Create(markerPath1)
		require.NoError(t, err)
		f.Close()

		os.Remove(rp.markerFilePath(mockNsMetadata, 0, blockTime, mockRule2)) // Ensure no marker for rule2

		// Call Process
		err = processor.Process(context.NewBackground(), mockNsMetadata, 0, blockTime)
		require.NoError(t, err)

		// Check marker for Rule1 (still exists, Process shouldn't error or remove it)
		_, statErr1 := os.Stat(markerPath1)
		require.NoError(t, statErr1, "marker file for rule1 should still exist")

		// Check marker for Rule2 (should NOT exist)
		_, statErr2 := os.Stat(rp.markerFilePath(mockNsMetadata, 0, blockTime, mockRule2))
		require.True(t, os.IsNotExist(statErr2), "marker file for rule2 should NOT be created")

		// Cleanup marker
		os.Remove(markerPath1)
	})

	// --- Test Case 3: No rules apply (all too new) ---
	t.Run("NoRulesApply", func(t *testing.T) {
		os.Remove(rp.markerFilePath(mockNsMetadata, 0, blockTime, mockRule1))
		os.Remove(rp.markerFilePath(mockNsMetadata, 0, blockTime, mockRule2))

		// Make rules such that blockTime is too new for both
		futureRule1 := newTestRollupRuleOptions(ctrl, rule1Res, 5*time.Hour)
		futureRule2 := newTestRollupRuleOptions(ctrl, rule2Res, 6*time.Hour)
		futureRules := []retention.RollupRuleOptions{futureRule1, futureRule2}
		mockRetentionOpts.EXPECT().RollupRules().Return(futureRules).AnyTimes() // Override previous AnyTimes

		err := processor.Process(context.NewBackground(), mockNsMetadata, 0, blockTime)
		require.NoError(t, err)

		_, statErr1 := os.Stat(rp.markerFilePath(mockNsMetadata, 0, blockTime, futureRule1))
		require.True(t, os.IsNotExist(statErr1), "marker for futureRule1 should not exist")
		_, statErr2 := os.Stat(rp.markerFilePath(mockNsMetadata, 0, blockTime, futureRule2))
		require.True(t, os.IsNotExist(statErr2), "marker for futureRule2 should not exist")
	})
}
