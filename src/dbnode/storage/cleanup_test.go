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

package storage

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/os"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
	"go.uber.org/zap"
)

var (
	retentionOptions = retention.NewOptions()
	namespaceOptions = namespace.NewOptions()
)

// MockRollupProcessor is a mock of RollupProcessor interface
type MockRollupProcessor struct {
	ctrl     *gomock.Controller
	recorder *MockRollupProcessorMockRecorder
}

// MockRollupProcessorMockRecorder is the mock recorder for MockRollupProcessor
type MockRollupProcessorMockRecorder struct {
	mock *MockRollupProcessor
}

// NewMockRollupProcessor creates a new mock instance
func NewMockRollupProcessor(ctrl *gomock.Controller) *MockRollupProcessor {
	mock := &MockRollupProcessor{ctrl: ctrl}
	mock.recorder = &MockRollupProcessorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockRollupProcessor) EXPECT() *MockRollupProcessorMockRecorder {
	return m.recorder
}

// Process mocks the Process method
func (m *MockRollupProcessor) Process(ctx context.Context, ns namespace.Metadata, shardID uint32, blockTime xtime.UnixNano) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Process", ctx, ns, shardID, blockTime)
	ret0, _ := ret[0].(error)
	return ret0
}

// Process indicates an expected call of Process
func (mr *MockRollupProcessorMockRecorder) Process(ctx, ns, shardID, blockTime interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Process", reflect.TypeOf((*MockRollupProcessor)(nil).Process), ctx, ns, shardID, blockTime)
}

// NeedsRollup mocks the NeedsRollup method
func (m *MockRollupProcessor) NeedsRollup(ns namespace.Metadata, shardID uint32, blockTime xtime.UnixNano, rule retention.RollupRuleOptions) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NeedsRollup", ns, shardID, blockTime, rule)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NeedsRollup indicates an expected call of NeedsRollup
func (mr *MockRollupProcessorMockRecorder) NeedsRollup(ns, shardID, blockTime, rule interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NeedsRollup", reflect.TypeOf((*MockRollupProcessor)(nil).NeedsRollup), ns, shardID, blockTime, rule)
}


func TestCleanupManagerCleanupCommitlogsAndSnapshots(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	testBlockStart := xtime.Now().Truncate(2 * time.Hour)
	testSnapshotUUID0 := uuid.Parse("a6367b49-9c83-4706-bd5c-400a4a9ec77c")
	require.NotNil(t, testSnapshotUUID0)

	testSnapshotUUID1 := uuid.Parse("bed2156f-182a-47ea-83ff-0a55d34c8a82")
	require.NotNil(t, testSnapshotUUID1)

	testCommitlogFileIdentifier := persist.CommitLogFile{
		FilePath: "commitlog-filepath-1",
		Index:    1,
	}
	testSnapshotMetadataIdentifier1 := fs.SnapshotMetadataIdentifier{
		Index: 0,
		UUID:  testSnapshotUUID0,
	}
	testSnapshotMetadataIdentifier2 := fs.SnapshotMetadataIdentifier{
		Index: 1,
		UUID:  testSnapshotUUID1,
	}
	testSnapshotMetadata0 := fs.SnapshotMetadata{
		ID:                  testSnapshotMetadataIdentifier1,
		CommitlogIdentifier: testCommitlogFileIdentifier,
		MetadataFilePath:    "metadata-filepath-0",
		CheckpointFilePath:  "checkpoint-filepath-0",
	}
	testSnapshotMetadata1 := fs.SnapshotMetadata{
		ID:                  testSnapshotMetadataIdentifier2,
		CommitlogIdentifier: testCommitlogFileIdentifier,
		MetadataFilePath:    "metadata-filepath-1",
		CheckpointFilePath:  "checkpoint-filepath-1",
	}

	testCases := []struct {
		title                string
		snapshotMetadata     snapshotMetadataFilesFn
		commitlogs           commitLogFilesFn
		snapshots            snapshotFilesFn
		expectedDeletedFiles []string
		expectErr            bool
	}{
		{
			title: "Does nothing if no snapshot metadata files",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return nil, nil, nil
			},
		},
		{
			title: "Does not delete snapshots associated with the most recent snapshot metadata file",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata0}, nil, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return fs.FileSetFilesSlice{
					{
						ID: fs.FileSetFileIdentifier{
							Namespace:   namespace,
							BlockStart:  testBlockStart,
							Shard:       shard,
							VolumeIndex: 0,
						},
						AbsoluteFilePaths:  []string{fmt.Sprintf("/snapshots/%s/snapshot-filepath-%d", namespace, shard)},
						CachedSnapshotTime: testBlockStart,
						CachedSnapshotID:   testSnapshotUUID0,
					},
				}, nil
			},
			commitlogs: func(commitlog.Options) (persist.CommitLogFiles, []commitlog.ErrorWithPath, error) {
				return nil, nil, nil
			},
		},
		{
			title: "Deletes snapshots and metadata not associated with the most recent snapshot metadata file",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata0, testSnapshotMetadata1}, nil, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return fs.FileSetFilesSlice{
					{
						ID: fs.FileSetFileIdentifier{
							Namespace:   namespace,
							BlockStart:  testBlockStart,
							Shard:       shard,
							VolumeIndex: 0,
						},
						AbsoluteFilePaths:  []string{fmt.Sprintf("/snapshots/%s/snapshot-filepath-%d", namespace, shard)},
						CachedSnapshotTime: testBlockStart,
						CachedSnapshotID:   testSnapshotUUID0,
					},
				}, nil
			},
			commitlogs: func(commitlog.Options) (persist.CommitLogFiles, []commitlog.ErrorWithPath, error) {
				return nil, nil, nil
			},
			expectedDeletedFiles: []string{
				"/snapshots/ns0/snapshot-filepath-0",
				"/snapshots/ns0/snapshot-filepath-1",
				"/snapshots/ns0/snapshot-filepath-2",
				"/snapshots/ns1/snapshot-filepath-0",
				"/snapshots/ns1/snapshot-filepath-1",
				"/snapshots/ns1/snapshot-filepath-2",
				"/snapshots/ns2/snapshot-filepath-0",
				"/snapshots/ns2/snapshot-filepath-1",
				"/snapshots/ns2/snapshot-filepath-2",
				"metadata-filepath-0",
				"checkpoint-filepath-0",
			},
		},
		{
			title: "Deletes corrupt snapshot metadata",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata1}, []fs.SnapshotMetadataErrorWithPaths{
					{
						Error:              errors.New("some-error"),
						MetadataFilePath:   "metadata-filepath-0",
						CheckpointFilePath: "checkpoint-filepath-0",
					},
				}, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return nil, nil
			},
			commitlogs: func(commitlog.Options) (persist.CommitLogFiles, []commitlog.ErrorWithPath, error) {
				return nil, nil, nil
			},
			expectedDeletedFiles: []string{
				"metadata-filepath-0",
				"checkpoint-filepath-0",
			},
		},
		{
			title: "Deletes corrupt snapshot files",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata0}, nil, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return fs.FileSetFilesSlice{
					{
						ID: fs.FileSetFileIdentifier{
							Namespace:   namespace,
							BlockStart:  testBlockStart,
							Shard:       shard,
							VolumeIndex: 0,
						},
						AbsoluteFilePaths: []string{fmt.Sprintf("/snapshots/%s/snapshot-filepath-%d", namespace, shard)},
						// Zero these out so it will try to look them up and return an error, indicating the files
						// are corrupt.
						CachedSnapshotTime: 0,
						CachedSnapshotID:   nil,
					},
				}, nil
			},
			commitlogs: func(commitlog.Options) (persist.CommitLogFiles, []commitlog.ErrorWithPath, error) {
				return nil, nil, nil
			},
			expectedDeletedFiles: []string{
				"/snapshots/ns0/snapshot-filepath-0",
				"/snapshots/ns0/snapshot-filepath-1",
				"/snapshots/ns0/snapshot-filepath-2",
				"/snapshots/ns1/snapshot-filepath-0",
				"/snapshots/ns1/snapshot-filepath-1",
				"/snapshots/ns1/snapshot-filepath-2",
				"/snapshots/ns2/snapshot-filepath-0",
				"/snapshots/ns2/snapshot-filepath-1",
				"/snapshots/ns2/snapshot-filepath-2",
			},
		},
		{
			title: "Does not delete the commitlog identified in the most recent snapshot metadata file, or any with a higher index",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata0}, nil, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return nil, nil
			},
			commitlogs: func(commitlog.Options) (persist.CommitLogFiles, []commitlog.ErrorWithPath, error) {
				return persist.CommitLogFiles{
					{FilePath: "commitlog-file-0", Index: 0},
					// Index 1, the one pointed to bby testSnapshotMetdata1
					testCommitlogFileIdentifier,
					{FilePath: "commitlog-file-2", Index: 2},
				}, nil, nil
			},
			// Should only delete anything with an index lower than 1.
			expectedDeletedFiles: []string{"commitlog-file-0"},
		},
		{
			title: "Deletes all corrupt commitlog files",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata0}, nil, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return nil, nil
			},
			commitlogs: func(commitlog.Options) (persist.CommitLogFiles, []commitlog.ErrorWithPath, error) {
				return nil, []commitlog.ErrorWithPath{
					commitlog.NewErrorWithPath(errors.New("some-error-0"), "corrupt-commitlog-file-0"),
					commitlog.NewErrorWithPath(errors.New("some-error-1"), "corrupt-commitlog-file-1"),
				}, nil
			},
			// Should only delete anything with an index lower than 1.
			expectedDeletedFiles: []string{"corrupt-commitlog-file-0", "corrupt-commitlog-file-1"},
		},
		{
			title: "Handles errors listing snapshot files",
			snapshotMetadata: func(fs.Options) ([]fs.SnapshotMetadata, []fs.SnapshotMetadataErrorWithPaths, error) {
				return []fs.SnapshotMetadata{testSnapshotMetadata0}, nil, nil
			},
			snapshots: func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
				return nil, errors.New("some-error")
			},
			commitlogs: func(commitlog.Options) (persist.CommitLogFiles, []commitlog.ErrorWithPath, error) {
				return nil, []commitlog.ErrorWithPath{
					commitlog.NewErrorWithPath(errors.New("some-error-0"), "corrupt-commitlog-file-0"),
					commitlog.NewErrorWithPath(errors.New("some-error-1"), "corrupt-commitlog-file-1"),
				}, nil
			},
			// We still expect it to delete the commitlog files even though its going to return an error.
			expectedDeletedFiles: []string{"corrupt-commitlog-file-0", "corrupt-commitlog-file-1"},
			expectErr:            true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			ts := timeFor()
			rOpts := retention.NewOptions().
				SetRetentionPeriod(21600 * time.Second).
				SetBlockSize(7200 * time.Second)
			nsOpts := namespace.NewOptions().SetRetentionOptions(rOpts)

			namespaces := make([]databaseNamespace, 0, 3)
			shards := make([]databaseShard, 0, 3)
			for i := 0; i < 3; i++ {
				shard := NewMockdatabaseShard(ctrl)
				shard.EXPECT().ID().Return(uint32(i)).AnyTimes()
				shard.EXPECT().IsBootstrapped().Return(true).AnyTimes()
				shard.EXPECT().CleanupExpiredFileSets(gomock.Any()).Return(nil).AnyTimes()
				shard.EXPECT().CleanupCompactedFileSets().Return(nil).AnyTimes()

				shards = append(shards, shard)
			}

			for i := 0; i < 3; i++ {
				ns := NewMockdatabaseNamespace(ctrl)
				ns.EXPECT().ID().Return(ident.StringID(fmt.Sprintf("ns%d", i))).AnyTimes()
				ns.EXPECT().Options().Return(nsOpts).AnyTimes()
				ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
				ns.EXPECT().OwnedShards().Return(shards).AnyTimes()
				namespaces = append(namespaces, ns)
			}

			db := newMockdatabase(ctrl, namespaces...)
			db.EXPECT().OwnedNamespaces().Return(namespaces, nil).AnyTimes()
			mgr := newCleanupManager(db, newNoopFakeActiveLogs(), tally.NoopScope).(*cleanupManager)
			mgr.opts = mgr.opts.SetCommitLogOptions(
				mgr.opts.CommitLogOptions().
					SetBlockSize(rOpts.BlockSize()))

			mgr.snapshotMetadataFilesFn = tc.snapshotMetadata
			mgr.commitLogFilesFn = tc.commitlogs
			mgr.snapshotFilesFn = tc.snapshots

			var deletedFiles []string
			mgr.deleteFilesFn = func(files []string) error {
				deletedFiles = append(deletedFiles, files...)
				return nil
			}

			err := cleanup(mgr, ts)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectedDeletedFiles, deletedFiles)
		})
	}
}

func TestCleanupManagerNamespaceCleanupBootstrapped(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ts := timeFor()
	rOpts := retentionOptions.
		SetRetentionPeriod(21600 * time.Second).
		SetBlockSize(3600 * time.Second)
	nsOpts := namespaceOptions.
		SetRetentionOptions(rOpts).
		SetCleanupEnabled(true).
		SetIndexOptions(namespace.NewIndexOptions().
			SetEnabled(true).
			SetBlockSize(7200 * time.Second))

	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ID().Return(uint32(42)).AnyTimes()
	shard.EXPECT().IsBootstrapped().Return(true).AnyTimes()
	shard.EXPECT().CleanupExpiredFileSets(gomock.Eq(ts.Add(-rOpts.RetentionPeriod()))).Return(nil)
	shard.EXPECT().CleanupCompactedFileSets().Return(nil)

	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().ID().Return(ident.StringID("ns")).AnyTimes()
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()
	ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	ns.EXPECT().OwnedShards().Return([]databaseShard{shard}).AnyTimes()

	idx := NewMockNamespaceIndex(ctrl)
	ns.EXPECT().Index().Times(3).Return(idx, nil)

	nses := []databaseNamespace{ns}
	db := newMockdatabase(ctrl, ns)
	db.EXPECT().OwnedNamespaces().Return(nses, nil).AnyTimes()

	mgr := newCleanupManager(db, newNoopFakeActiveLogs(), nil, tally.NoopScope).(*cleanupManager)
	idx.EXPECT().CleanupExpiredFileSets(ts).Return(nil)
	idx.EXPECT().CleanupCorruptedFileSets().Return(nil)
	idx.EXPECT().CleanupDuplicateFileSets([]uint32{42}).Return(nil)
	require.NoError(t, cleanup(mgr, ts))
}

func TestCleanupManager_ProcessRollups(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDb := NewMockdatabase(ctrl)
	mockActiveLogs := newNoopFakeActiveLogs()
	mockRollupProcessor := NewMockRollupProcessor(ctrl)
	testScope := tally.NewTestScope("test", nil)

	now := xtime.Now()
	blockTime1 := now.Add(-2 * time.Hour)
	blockTime2 := now.Add(-4 * time.Hour)

	// Mock namespace and shards
	mockShard1 := NewMockdatabaseShard(ctrl)
	mockShard1.EXPECT().ID().Return(uint32(1)).AnyTimes()

	mockShard2 := NewMockdatabaseShard(ctrl)
	mockShard2.EXPECT().ID().Return(uint32(2)).AnyTimes()

	nsOpts := namespace.NewOptions().
		SetRetentionOptions(retention.NewOptions().SetBlockSize(2 * time.Hour)).
		SetCleanupEnabled(true).
		SetContextOptions(context.NewOptions()) // Ensure ContextOptions is not nil

	nsOpts.RetentionOptions().SetRollupRules([]retention.RollupRuleOptions{
		// Add a dummy rule to make sure rollup processing is attempted
		retention.NewMockRollupRuleOptions(ctrl),
	})


	mockNs1 := NewMockdatabaseNamespace(ctrl)
	mockNs1.EXPECT().ID().Return(ident.StringID("ns1")).AnyTimes()
	mockNs1.EXPECT().Options().Return(nsOpts).AnyTimes()
	mockNs1.EXPECT().OwnedShards().Return([]databaseShard{mockShard1}).AnyTimes()

	mockNs2 := NewMockdatabaseNamespace(ctrl)
	mockNs2.EXPECT().ID().Return(ident.StringID("ns2")).AnyTimes()
	mockNs2.EXPECT().Options().Return(nsOpts).AnyTimes() // Same options for simplicity
	mockNs2.EXPECT().OwnedShards().Return([]databaseShard{mockShard2}).AnyTimes()

	namespaces := []databaseNamespace{mockNs1, mockNs2}
	mockDb.EXPECT().OwnedNamespaces().Return(namespaces, nil).AnyTimes()
	mockDb.EXPECT().Options().Return(DefaultTestOptions()).AnyTimes() // For fsOpts

	cm := newCleanupManager(mockDb, mockActiveLogs, mockRollupProcessor, testScope).(*cleanupManager)

	// Mock fs.ReadInfoFiles
	originalReadInfoFilesFn := fs.ReadInfoFiles // Save original
	fs.ReadInfoFiles = func(opts fs.ReadInfoFilesOptions) ([]fs.ReadInfoFileResult, error) {
		results := []fs.ReadInfoFileResult{}
		if opts.Namespace.String() == "ns1" && opts.Shard == 1 {
			results = append(results, fs.ReadInfoFileResult{Info: persist.NewInfoFromFields(int64(blockTime1), 0, 0, 0, 0, 0)})
		}
		if opts.Namespace.String() == "ns2" && opts.Shard == 2 {
			results = append(results, fs.ReadInfoFileResult{Info: persist.NewInfoFromFields(int64(blockTime2), 0, 0, 0, 0, 0)})
		}
		return results, nil
	}
	defer func() { fs.ReadInfoFiles = originalReadInfoFilesFn }() // Restore

	// Expectations for RollupProcessor.Process
	// Use gomock.Any() for context.Context as it's tricky to match precisely
	mockRollupProcessor.EXPECT().Process(gomock.Any(), mockNs1, uint32(1), blockTime1).Return(nil).Times(1)
	mockRollupProcessor.EXPECT().Process(gomock.Any(), mockNs2, uint32(2), blockTime2).Return(nil).Times(1)

	err := cm.processRollups(namespaces)
	require.NoError(t, err)
}

func TestCleanupManager_CleanupExpiredRollupDataFiles(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	testScope := tally.NewTestScope("test_cleanup_expired_rollup", nil)
	logger := zap.NewNop() // Or zaptest.NewLogger(t) for output

	// Setup temp directory structure
	tempBaseDir, err := os.MkdirTemp("", "cleanup_rollup_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempBaseDir)

	nsID := ident.StringID("testns_expired_rollup")
	shardID := uint32(0)

	// Mock database and options
	mockDb := NewMockdatabase(ctrl)
	commitLogOpts := commitlog.NewOptions().SetFilesystemOptions(
		fs.NewOptions().SetFilePathPrefix(tempBaseDir),
	)
	dbOpts := DefaultTestOptions().SetCommitLogOptions(commitLogOpts)
	mockDb.EXPECT().Options().Return(dbOpts).AnyTimes()

	// Create cleanupManager instance
	// RollupProcessor is not used by cleanupExpiredRollupDataFiles directly, so can be nil or a simple mock
	mockRollupProc := NewMockRollupProcessor(ctrl)
	cm := newCleanupManager(mockDb, newNoopFakeActiveLogs(), mockRollupProc, testScope).(*cleanupManager)
	cm.logger = logger // Assign logger

	// --- Directory structure ---
	// tempBaseDir/
	//   testns_expired_rollup/
	//     0/  (shardID)
	//       1000/ (blockTime: expired)
	//         rollup_1m/ (should be deleted)
	//           info.db (dummy file)
	//         rollup_5m/ (should be deleted)
	//           info.db (dummy file)
	//         raw_data_files... (not touched by this specific function)
	//       2000/ (blockTime: not expired)
	//         rollup_1m/ (should NOT be deleted)
	//           info.db (dummy file)
	//       3000/ (blockTime: expired, but no rollup dir)
	//         raw_data_files...
	//       4000/ (blockTime: expired)
	//         not_a_rollup_dir/ (should NOT be deleted)
	//           info.db
	//         rollup_10m/ (should be deleted)
	//           info.db
	//---------------------------

	now := xtime.Now()
	expiredBlockTime1 := now.Add(-10 * time.Hour) // 1000 in our made-up nanosecond scale
	notExpiredBlockTime := now.Add(-1 * time.Hour)  // 2000
	expiredBlockTime2 := now.Add(-12 * time.Hour) // 3000
	expiredBlockTime3 := now.Add(-14 * time.Hour) // 4000

	// Retention period: 5 hours. So anything older than 5 hours from 'now' is expired.
	// earliestToRetain = now - 5h
	retentionPeriod := 5 * time.Hour
	earliestToRetain := now.Add(-retentionPeriod)

	// Create directories and dummy files
	shardPath := fs.ShardDirPath(tempBaseDir, nsID, shardID)

	// Block 1000 (expired)
	block1000Path := filepath.Join(shardPath, fmt.Sprintf("%d", expiredBlockTime1.UnixNano()))
	rollup1mPathB1 := filepath.Join(block1000Path, "rollup_1m")
	rollup5mPathB1 := filepath.Join(block1000Path, "rollup_5m")
	require.NoError(t, os.MkdirAll(rollup1mPathB1, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(rollup1mPathB1, "info.db"), []byte("test"), 0644))
	require.NoError(t, os.MkdirAll(rollup5mPathB1, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(rollup5mPathB1, "info.db"), []byte("test"), 0644))

	// Block 2000 (not expired)
	block2000Path := filepath.Join(shardPath, fmt.Sprintf("%d", notExpiredBlockTime.UnixNano()))
	rollup1mPathB2 := filepath.Join(block2000Path, "rollup_1m")
	require.NoError(t, os.MkdirAll(rollup1mPathB2, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(rollup1mPathB2, "info.db"), []byte("test"), 0644))

	// Block 3000 (expired, no rollup dir)
	block3000Path := filepath.Join(shardPath, fmt.Sprintf("%d", expiredBlockTime2.UnixNano()))
	require.NoError(t, os.MkdirAll(block3000Path, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(block3000Path, "raw.db"), []byte("test"), 0644))

	// Block 4000 (expired, one rollup, one not)
	block4000Path := filepath.Join(shardPath, fmt.Sprintf("%d", expiredBlockTime3.UnixNano()))
	notRollupPathB4 := filepath.Join(block4000Path, "not_a_rollup_dir")
	rollup10mPathB4 := filepath.Join(block4000Path, "rollup_10m")
	require.NoError(t, os.MkdirAll(notRollupPathB4, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(notRollupPathB4, "info.db"), []byte("test"), 0644))
	require.NoError(t, os.MkdirAll(rollup10mPathB4, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(rollup10mPathB4, "info.db"), []byte("test"), 0644))


	// Mock namespace and shard objects
	mockNs := NewMockdatabaseNamespace(ctrl)
	mockNs.EXPECT().ID().Return(nsID).AnyTimes()
	// Options needed by cleanupExpiredRollupDataFiles for fsOpts
	mockNs.EXPECT().Options().Return(namespace.NewOptions().SetRetentionOptions(retention.NewOptions().SetRetentionPeriod(retentionPeriod))).AnyTimes()


	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().ID().Return(shardID).AnyTimes()

	// Execute the function
	err = cm.cleanupExpiredRollupDataFiles(earliestToRetain, mockNs, []databaseShard{mockShard})
	require.NoError(t, err)

	// Assertions
	// Expired and should be deleted
	_, err = os.Stat(rollup1mPathB1)
	require.True(t, os.IsNotExist(err), "rollup_1m for expired block 1000 should be deleted")
	_, err = os.Stat(rollup5mPathB1)
	require.True(t, os.IsNotExist(err), "rollup_5m for expired block 1000 should be deleted")
	_, err = os.Stat(rollup10mPathB4)
	require.True(t, os.IsNotExist(err), "rollup_10m for expired block 4000 should be deleted")

	// Not expired, should still exist
	_, err = os.Stat(rollup1mPathB2)
	require.NoError(t, err, "rollup_1m for not-expired block 2000 should exist")

	// Not a rollup dir, should still exist
	_, err = os.Stat(notRollupPathB4)
	require.NoError(t, err, "not_a_rollup_dir for expired block 4000 should exist")

	// Raw data in expired block 3000 should still exist (not touched by this func)
	_, err = os.Stat(filepath.Join(block3000Path, "raw.db"))
	require.NoError(t, err, "raw.db for expired block 3000 should exist")
}


func TestCleanupManagerNamespaceCleanupNotBootstrapped(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ts := timeFor()
	rOpts := retentionOptions.
		SetRetentionPeriod(21600 * time.Second).
		SetBlockSize(3600 * time.Second)
	nsOpts := namespaceOptions.
		SetRetentionOptions(rOpts).
		SetCleanupEnabled(true).
		SetIndexOptions(namespace.NewIndexOptions().
			SetEnabled(true).
			SetBlockSize(7200 * time.Second))

	idx := NewMockNamespaceIndex(ctrl)
	idx.EXPECT().CleanupExpiredFileSets(gomock.Any()).Return(nil)
	idx.EXPECT().CleanupCorruptedFileSets().Return(nil)
	idx.EXPECT().CleanupDuplicateFileSets(gomock.Any()).Return(nil)

	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().ID().Return(ident.StringID("ns")).AnyTimes()
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()
	ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	ns.EXPECT().OwnedShards().Return(nil).AnyTimes()
	ns.EXPECT().Index().Return(idx, nil).AnyTimes()

	nses := []databaseNamespace{ns}
	db := newMockdatabase(ctrl, ns)
	db.EXPECT().OwnedNamespaces().Return(nses, nil).AnyTimes()

	mgr := newCleanupManager(db, newNoopFakeActiveLogs(), nil, tally.NoopScope).(*cleanupManager)
	require.NoError(t, cleanup(mgr, ts))
}

// Test NS doesn't cleanup when flag is present
func TestCleanupManagerDoesntNeedCleanup(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	ts := timeFor()
	rOpts := retentionOptions.
		SetRetentionPeriod(21600 * time.Second).
		SetBlockSize(7200 * time.Second)
	nsOpts := namespaceOptions.SetRetentionOptions(rOpts).SetCleanupEnabled(false)

	namespaces := make([]databaseNamespace, 0, 3)
	for range namespaces {
		ns := NewMockdatabaseNamespace(ctrl)
		ns.EXPECT().Options().Return(nsOpts).AnyTimes()
		ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
		namespaces = append(namespaces, ns)
	}
	db := newMockdatabase(ctrl, namespaces...)
	db.EXPECT().OwnedNamespaces().Return(namespaces, nil).AnyTimes()
	mgr := newCleanupManager(db, newNoopFakeActiveLogs(), tally.NoopScope).(*cleanupManager)
	mgr.opts = mgr.opts.SetCommitLogOptions(
		mgr.opts.CommitLogOptions().
			SetBlockSize(rOpts.BlockSize()))

	var deletedFiles []string
	mgr.deleteFilesFn = func(files []string) error {
		deletedFiles = append(deletedFiles, files...)
		return nil
	}

	require.NoError(t, cleanup(mgr, ts))
}

func TestCleanupDataAndSnapshotFileSetFiles(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	ts := timeFor()

	nsOpts := namespaceOptions
	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()

	shard := NewMockdatabaseShard(ctrl)
	shardNotBootstrapped := NewMockdatabaseShard(ctrl)
	shardNotBootstrapped.EXPECT().IsBootstrapped().Return(false).AnyTimes()
	shardNotBootstrapped.EXPECT().ID().Return(uint32(1)).AnyTimes()
	expectedEarliestToRetain := retention.FlushTimeStart(ns.Options().RetentionOptions(), ts)
	shard.EXPECT().IsBootstrapped().Return(true).AnyTimes()
	shard.EXPECT().CleanupExpiredFileSets(expectedEarliestToRetain).Return(nil)
	shard.EXPECT().CleanupCompactedFileSets().Return(nil)
	shard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	ns.EXPECT().OwnedShards().Return([]databaseShard{shard, shardNotBootstrapped}).AnyTimes()
	ns.EXPECT().ID().Return(ident.StringID("nsID")).AnyTimes()
	ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	namespaces := []databaseNamespace{ns}

	db := newMockdatabase(ctrl, namespaces...)
	db.EXPECT().OwnedNamespaces().Return(namespaces, nil).AnyTimes()
	mgr := newCleanupManager(db, newNoopFakeActiveLogs(), tally.NoopScope).(*cleanupManager)

	require.NoError(t, cleanup(mgr, ts))
}

type deleteInactiveDirectoriesCall struct {
	parentDirPath  string
	activeDirNames []string
}

func TestDeleteInactiveDataAndSnapshotFileSetFiles(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	ts := timeFor()

	nsOpts := namespaceOptions.
		SetCleanupEnabled(false)
	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(nsOpts).AnyTimes()

	shard := NewMockdatabaseShard(ctrl)
	shard.EXPECT().ID().Return(uint32(0)).AnyTimes()
	shard.EXPECT().IsBootstrapped().Return(true).AnyTimes()
	ns.EXPECT().OwnedShards().Return([]databaseShard{shard}).AnyTimes()
	ns.EXPECT().ID().Return(ident.StringID("nsID")).AnyTimes()
	ns.EXPECT().NeedsFlush(gomock.Any(), gomock.Any()).Return(false, nil).AnyTimes()
	namespaces := []databaseNamespace{ns}

	db := newMockdatabase(ctrl, namespaces...)
	db.EXPECT().OwnedNamespaces().Return(namespaces, nil).AnyTimes()
	mgr := newCleanupManager(db, newNoopFakeActiveLogs(), tally.NoopScope).(*cleanupManager)

	deleteInactiveDirectoriesCalls := []deleteInactiveDirectoriesCall{}
	deleteInactiveDirectoriesFn := func(parentDirPath string, activeDirNames []string) error {
		deleteInactiveDirectoriesCalls = append(deleteInactiveDirectoriesCalls, deleteInactiveDirectoriesCall{
			parentDirPath:  parentDirPath,
			activeDirNames: activeDirNames,
		})
		return nil
	}
	mgr.deleteInactiveDirectoriesFn = deleteInactiveDirectoriesFn

	require.NoError(t, cleanup(mgr, ts))

	expectedCalls := []deleteInactiveDirectoriesCall{
		{
			parentDirPath:  "data/nsID",
			activeDirNames: []string{"0"},
		},
		{
			parentDirPath:  "snapshots/nsID",
			activeDirNames: []string{"0"},
		},
		{
			parentDirPath:  "data",
			activeDirNames: []string{"nsID"},
		},
	}

	for _, expectedCall := range expectedCalls {
		found := false
		for _, call := range deleteInactiveDirectoriesCalls {
			if strings.Contains(call.parentDirPath, expectedCall.parentDirPath) &&
				expectedCall.activeDirNames[0] == call.activeDirNames[0] {
				found = true
			}
		}
		require.Equal(t, true, found)
	}
}

func TestCleanupManagerPropagatesOwnedNamespacesError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	ts := timeFor()

	db := NewMockdatabase(ctrl)
	db.EXPECT().Options().Return(DefaultTestOptions()).AnyTimes()
	db.EXPECT().Open().Return(nil)
	db.EXPECT().Terminate().Return(nil)
	db.EXPECT().OwnedNamespaces().Return(nil, errDatabaseIsClosed).AnyTimes()

	mgr := newCleanupManager(db, newNoopFakeActiveLogs(), tally.NoopScope).(*cleanupManager)
	require.NoError(t, db.Open())
	require.NoError(t, db.Terminate())

	require.Error(t, cleanup(mgr, ts))
}

func timeFor() xtime.UnixNano {
	return xtime.FromSeconds(36000)
}

type fakeActiveLogs struct {
	activeLogs persist.CommitLogFiles
}

func (f fakeActiveLogs) ActiveLogs() (persist.CommitLogFiles, error) {
	return f.activeLogs, nil
}

func newNoopFakeActiveLogs() fakeActiveLogs {
	return newFakeActiveLogs(nil)
}

func newFakeActiveLogs(activeLogs persist.CommitLogFiles) fakeActiveLogs {
	return fakeActiveLogs{
		activeLogs: activeLogs,
	}
}

func cleanup(
	mgr databaseCleanupManager,
	t xtime.UnixNano,
) error {
	multiErr := xerrors.NewMultiError()
	multiErr = multiErr.Add(mgr.WarmFlushCleanup(t))
	multiErr = multiErr.Add(mgr.ColdFlushCleanup(t))
	return multiErr.FinalError()
}
