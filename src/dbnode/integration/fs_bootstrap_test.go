//go:build integration
// +build integration

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

package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/encoding"
)

func TestFilesystemBootstrap(t *testing.T) {
	testFilesystemBootstrap(t, nil, nil)
}

func TestProtoFilesystemBootstrap(t *testing.T) {
	testFilesystemBootstrap(t, setProtoTestOptions, setProtoTestInputConfig)
}

func TestM3TSZAdvancedFilesystemBootstrap(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	var (
		blockSize = 2 * time.Hour
		rOpts     = retention.NewOptions().SetRetentionPeriod(2 * time.Hour).SetBlockSize(blockSize)
	)

	// Create encoding options for M3TSZ-Advanced
	advancedEncodingOpts := encoding.NewOptions().SetEncodingType(encoding.M3TSZAdvancedEncoding)

	// Create namespace options with M3TSZ-Advanced encoding for ns1
	ns1Opts := namespace.NewOptions().
		SetRetentionOptions(rOpts).
		SetEncodingOptions(advancedEncodingOpts)
	ns1, err := namespace.NewMetadata(testNamespaces[0], ns1Opts)
	require.NoError(t, err)

	// ns2 will use default encoding options for comparison or to ensure system stability
	ns2Opts := namespace.NewOptions().SetRetentionOptions(rOpts)
	ns2, err := namespace.NewMetadata(testNamespaces[1], ns2Opts)
	require.NoError(t, err)

	testOpts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1, ns2})

	// Test setup (derived from testFilesystemBootstrap)
	setup, err := NewTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer setup.Close()

	require.NoError(t, setup.InitializeBootstrappers(InitializeBootstrappersOptions{
		WithFileSystem: true,
	}))

	// Write test data
	now := setup.NowFn()()
	// Define data that can test M3TSZ-Advanced features
	// For example, data that can trigger RLE for timestamps and values
	inputDataNs1 := []generate.BlockConfig{
		{IDs: []string{"fooAdv", "barAdv"}, NumPoints: 100, Start: now.Add(-blockSize),
			// Example of how to make data regular for RLE testing if generate supports it:
			// TimeStep: time.Second * 10, ValueStep: 0 (for value RLE)
		},
		{IDs: []string{"fooAdv", "bazAdv"}, NumPoints: 50, Start: now,
			// ValueStep: 0.0001 (for small XOR diffs)
		},
		// Add more diverse blocks: large deltas, NaNs, specific LZ/TZ changes for values
		{IDs: []string{"rleBoth"}, NumPoints: 10, Start: now.Add(-blockSize / 2), TimeStep: time.Minute, ValueGen: func(i int) float64 { return 123.45 }},
		{IDs: []string{"nanSeries"}, NumPoints: 5, Start: now.Add(-blockSize / 3), TimeStep: time.Minute, ValueGen: func(i int) float64 { if i % 2 == 0 {return float64(i)} else {return math.NaN()} }},

	}
	seriesMapsNs1 := generate.BlocksByStart(inputDataNs1)
	require.NoError(t, writeTestDataToDisk(ns1, setup, seriesMapsNs1, 0))

	// For ns2 (default encoding), you can write similar or different data
	inputDataNs2 := []generate.BlockConfig{
		{IDs: []string{"fooDef", "barDef"}, NumPoints: 80, Start: now.Add(-blockSize)},
	}
	seriesMapsNs2 := generate.BlocksByStart(inputDataNs2)
	require.NoError(t, writeTestDataToDisk(ns2, setup, seriesMapsNs2, 0))


	// Start the server with filesystem bootstrapper
	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Debug("M3TSZ-Advanced filesystem bootstrap test")
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	// Verify in-memory data match what we expect
	verifySeriesMaps(t, setup, ns1.ID().String(), seriesMapsNs1) // Use ns1.ID()
	verifySeriesMaps(t, setup, ns2.ID().String(), seriesMapsNs2) // Use ns2.ID()
}


func testFilesystemBootstrap(t *testing.T, setTestOpts setTestOptions, updateInputConfig generate.UpdateBlockConfig) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		blockSize = 2 * time.Hour
		rOpts     = retention.NewOptions().SetRetentionPeriod(2 * time.Hour).SetBlockSize(blockSize)
	)
	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(rOpts))
	require.NoError(t, err)
	ns2, err := namespace.NewMetadata(testNamespaces[1], namespace.NewOptions().SetRetentionOptions(rOpts))
	require.NoError(t, err)

	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1, ns2})
	if setTestOpts != nil {
		opts = setTestOpts(t, opts)
		// ns1 and ns2 are updated based on opts, so no need to re-assign from opts.Namespaces()
		// if setTestOpts modifies the opts in place or returns the modified one which is assigned to opts.
	}

	// Test setup
	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	require.NoError(t, setup.InitializeBootstrappers(InitializeBootstrappersOptions{
		WithFileSystem: true,
	}))

	// Write test data
	now := setup.NowFn()()
	inputData := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 100, Start: now.Add(-blockSize)},
		{IDs: []string{"foo", "baz"}, NumPoints: 50, Start: now},
	}
	if updateInputConfig != nil {
		updateInputConfig(inputData)
	}
	seriesMaps := generate.BlocksByStart(inputData)
	require.NoError(t, writeTestDataToDisk(ns1, setup, seriesMaps, 0))
	require.NoError(t, writeTestDataToDisk(ns2, setup, nil, 0))

	// Start the server with filesystem bootstrapper
	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Debug("filesystem bootstrap test")
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	// Verify in-memory data match what we expect
	// Note: If setTestOpts is nil, ns1 and ns2 are from the initial creation.
	// If setTestOpts is not nil, it's assumed to update the namespaces within the opts object,
	// and those are the ones that should be used for verification.
	currentNs1 := opts.Namespaces()[0]
	currentNs2 := opts.Namespaces()[1]
	verifySeriesMaps(t, setup, currentNs1.ID().String(), seriesMaps)
	verifySeriesMaps(t, setup, currentNs2.ID().String(), nil)
}
