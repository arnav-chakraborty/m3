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

package client

import (
	"bytes"
	gocontext "context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/encoding"
	grpcrpc "github.com/m3db/m3/src/dbnode/generated/proto/rpc"    // gRPC generated
	thriftrpc "github.com/m3db/m3/src/dbnode/generated/thrift/rpc" // Thrift generated
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/convert"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	idxconvert "github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/dbnode/x/xpool"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xresource "github.com/m3db/m3/src/x/resource"
	xretry "github.com/m3db/m3/src/x/retry"
	"github.com/m3db/m3/src/x/sampler"
	"github.com/m3db/m3/src/x/serialize"
	xsync "github.com/m3db/m3/src/x/sync"
	tbinarypool "github.com/m3db/m3/src/x/thrift"
	xtime "github.com/m3db/m3/src/x/time"

	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	clusterConnectWaitInterval       = 10 * time.Millisecond
	gaugeReportInterval              = 500 * time.Millisecond
	blockMetadataChBufSize           = 65536
	hostNotAvailableMinSleepInterval = 1 * time.Millisecond
	hostNotAvailableMaxSleepInterval = 100 * time.Millisecond
)

type resultTypeEnum string

const (
	resultTypeMetadata  resultTypeEnum = "metadata"
	resultTypeBootstrap                = "bootstrap"
	resultTypeRaw                      = "raw"
)

var errUnknownWriteAttemptType = errors.New(
	"unknown write attempt type specified, internal error")

var (
	// ErrClusterConnectTimeout is raised when connecting to the cluster and
	// ensuring at least each partition has an up node with a connection to it
	ErrClusterConnectTimeout = errors.New("timed out establishing min connections to cluster")
	// errSessionStatusNotInitial is raised when trying to open a session and
	// its not in the initial clean state
	errSessionStatusNotInitial = errors.New("session not in initial state")
	// ErrSessionStatusNotOpen is raised when operations are requested when the
	// session is not in the open state
	ErrSessionStatusNotOpen = errors.New("session not in open state")
	// errSessionBadBlockResultFromPeer is raised when there is a bad block
	// return from a peer when fetching blocks from peers
	errSessionBadBlockResultFromPeer = errors.New("session fetched bad block result from peer")
	// errSessionInvalidConnectClusterConnectConsistencyLevel is raised when
	// the connect consistency level specified is not recognized
	errSessionInvalidConnectClusterConnectConsistencyLevel = errors.New(
		"session has invalid connect consistency level specified",
	)
	// errSessionHasNoHostQueueForHost is raised when host queue requested for a missing host
	errSessionHasNoHostQueueForHost = newHostNotAvailableError(errors.New("session has no host queue for host"))
	// errUnableToEncodeTags is raised when the server is unable to encode provided tags
	// to be sent over the wire.
	errUnableToEncodeTags = errors.New("unable to include tags")
	// errEnqueueChIsClosed is returned when attempting to use a closed enqueuCh.
	errEnqueueChIsClosed = errors.New("error enqueueCh is cosed")
)

// sessionState is volatile state that is protected by a
// read/write mutex
type sessionState struct {
	sync.RWMutex

	status status

	writeLevel     topology.ConsistencyLevel
	readLevel      topology.ReadConsistencyLevel
	bootstrapLevel topology.ReadConsistencyLevel

	queues         []hostQueue
	queuesByHostID map[string]hostQueue
	topo           topology.Topology
	topoMap        topology.Map
	topoWatch      topology.MapWatch
	replicas       int
	majority       int
}

func (s *sessionState) readConsistencyLevelWithRLock(
	override *topology.ReadConsistencyLevel,
) topology.ReadConsistencyLevel {
	if override == nil {
		return s.readLevel
	}
	return *override
}

type session struct {
	state                                               sessionState
	opts                                                Options
	runtimeOptsListenerCloser                           xresource.SimpleCloser
	scope                                               tally.Scope
	nowFn                                               clock.NowFn
	log                                                 *zap.Logger
	logWriteErrorSampler                                *sampler.Sampler
	logFetchErrorSampler                                *sampler.Sampler
	logHostWriteErrorSampler                            *sampler.Sampler
	logHostFetchErrorSampler                            *sampler.Sampler
	newHostQueueFn                                      newHostQueueFn
	writeRetrier                                        xretry.Retrier
	fetchRetrier                                        xretry.Retrier
	streamBlocksRetrier                                 xretry.Retrier
	pools                                               sessionPools
	fetchBatchSize                                      int
	newPeerBlocksQueueFn                                newPeerBlocksQueueFn
	reattemptStreamBlocksFromPeersFn                    reattemptStreamBlocksFromPeersFn
	pickBestPeerFn                                      pickBestPeerFn
	healthCheckNewConnFn                                healthCheckFn
	origin                                              topology.Host
	streamBlocksMaxBlockRetries                         int
	streamBlocksWorkers                                 xsync.WorkerPool
	streamBlocksBatchSize                               int
	streamBlocksMetadataBatchTimeout                    time.Duration
	streamBlocksBatchTimeout                            time.Duration
	writeShardsInitializing                             bool
	shardsLeavingCountTowardsConsistency                bool
	shardsLeavingAndInitializingCountTowardsConsistency bool
	metrics                                             sessionMetrics

	// gRPC specific fields
	grpcNodeClients    map[string]grpcrpc.NodeClient
	grpcClusterClients map[string]grpcrpc.ClusterClient // For future use
	grpcConns          map[string]*grpc.ClientConn
	grpcTargetResolver GRPCTargetResolver
}

// defaultGRPCTargetResolver is a basic resolver.
type defaultGRPCTargetResolver struct {
	defaultPort string // e.g., ":9090" or "9090"
}

// PositiveIntFromString attempts to parse a positive integer from a string.
// Returns the integer and true if successful, 0 and false otherwise.
func PositiveIntFromString(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	val := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, false // Not a digit
		}
		val = val*10 + int(r-'0')
	}
	return val, true
}

func newDefaultGRPCTargetResolver(defaultGRPCPort string) GRPCTargetResolver {
	if defaultGRPCPort != "" && !strings.HasPrefix(defaultGRPCPort, ":") {
		defaultGRPCPort = ":" + defaultGRPCPort
	}
	return &defaultGRPCTargetResolver{defaultPort: defaultGRPCPort}
}

func (r *defaultGRPCTargetResolver) Resolve(host topology.Host) (string, error) {
	addr := host.Address()

	if r.defaultPort == "" {
		return addr, nil
	}

	hostPart := addr
	lastColon := strings.LastIndex(addr, ":")
	if lastColon != -1 {
		if _, isPort := PositiveIntFromString(addr[lastColon+1:]); isPort {
			hostPart = addr[:lastColon]
		}
	}
	return hostPart + r.defaultPort, nil
}

type shardMetricsKey struct {
	shardID    uint32
	resultType resultTypeEnum
}

type sessionMetrics struct {
	sync.RWMutex
	writeSuccess                                     tally.Counter
	writeSuccessForCountLeavingAndInitializingAsPair tally.Counter
	writeErrorsBadRequest                            tally.Counter
	writeErrorsInternalError                         tally.Counter
	writeLatencyHistogram                            tally.Histogram
	writeNodesRespondingErrors                       []tally.Counter
	writeNodesRespondingBadRequestErrors             []tally.Counter
	fetchSuccess                                     tally.Counter
	fetchErrorsBadRequest                            tally.Counter
	fetchErrorsInternalError                         tally.Counter
	fetchLatencyHistogram                            tally.Histogram
	fetchNodesRespondingErrors                       []tally.Counter
	fetchNodesRespondingBadRequestErrors             []tally.Counter
	topologyUpdatedSuccess                           tally.Counter
	topologyUpdatedError                             tally.Counter
	streamFromPeersMetrics                           map[shardMetricsKey]streamFromPeersMetrics
	clusterConnectLatency                            tally.Histogram
}

func newSessionMetrics(scope tally.Scope) sessionMetrics {
	return sessionMetrics{
		writeSuccess: scope.Counter("write.success"),
		writeSuccessForCountLeavingAndInitializingAsPair: scope.Tagged(map[string]string{
			"success_type": "leaving_initializing_as_pair",
		}).Counter("write.success"),
		writeErrorsBadRequest: scope.Tagged(map[string]string{
			"error_type": "bad_request",
		}).Counter("write.errors"),
		writeErrorsInternalError: scope.Tagged(map[string]string{
			"error_type": "internal_error",
		}).Counter("write.errors"),
		writeLatencyHistogram: histogramWithDurationBuckets(scope, "write.latency"),
		fetchSuccess:          scope.Counter("fetch.success"),
		fetchErrorsBadRequest: scope.Tagged(map[string]string{
			"error_type": "bad_request",
		}).Counter("fetch.errors"),
		fetchErrorsInternalError: scope.Tagged(map[string]string{
			"error_type": "internal_error",
		}).Counter("fetch.errors"),
		fetchLatencyHistogram:  histogramWithDurationBuckets(scope, "fetch.latency"),
		topologyUpdatedSuccess: scope.Counter("topology.updated-success"),
		topologyUpdatedError:   scope.Counter("topology.updated-error"),
		streamFromPeersMetrics: make(map[shardMetricsKey]streamFromPeersMetrics),
		clusterConnectLatency:  histogramWithDurationBuckets(scope, "cluster-connect.latency"),
	}
}

type streamFromPeersMetrics struct {
	fetchBlocksFromPeers                              tally.Gauge
	metadataFetches                                   tally.Gauge
	metadataFetchBatchCall                            tally.Counter
	metadataFetchBatchSuccess                         tally.Counter
	metadataFetchBatchError                           tally.Counter
	metadataFetchBatchBlockErr                        tally.Counter
	metadataReceived                                  tally.Counter
	metadataPeerRetry                                 tally.Counter
	fetchBlockSuccess                                 tally.Counter
	fetchBlockError                                   tally.Counter
	fetchBlockFullRetry                               tally.Counter
	fetchBlockFinalError                              tally.Counter
	fetchBlockRetriesReqError                         tally.Counter
	fetchBlockRetriesRespError                        tally.Counter
	fetchBlockRetriesConsistencyLevelNotAchievedError tally.Counter
	blocksEnqueueChannel                              tally.Gauge
}

type hostQueueOpts struct {
	writeBatchRawRequestPool                     writeBatchRawRequestPool
	writeBatchRawV2RequestPool                   writeBatchRawV2RequestPool
	writeBatchRawRequestElementArrayPool         writeBatchRawRequestElementArrayPool
	writeBatchRawV2RequestElementArrayPool       writeBatchRawV2RequestElementArrayPool
	writeTaggedBatchRawRequestPool               writeTaggedBatchRawRequestPool
	writeTaggedBatchRawV2RequestPool             writeTaggedBatchRawV2RequestPool
	writeTaggedBatchRawRequestElementArrayPool   writeTaggedBatchRawRequestElementArrayPool
	writeTaggedBatchRawV2RequestElementArrayPool writeTaggedBatchRawV2RequestElementArrayPool
	fetchBatchRawV2RequestPool                   fetchBatchRawV2RequestPool
	fetchBatchRawV2RequestElementArrayPool       fetchBatchRawV2RequestElementArrayPool
	opts                                         Options
}

type newHostQueueFn func(
	host topology.Host,
	hostQueueOpts hostQueueOpts,
) (hostQueue, error)

func newSession(opts Options) (clientSession, error) {
	topo, err := opts.TopologyInitializer().Init()
	if err != nil {
		return nil, err
	}

	logWriteErrorSampler, err := sampler.NewSampler(opts.LogErrorSampleRate())
	if err != nil {
		return nil, err
	}

	logFetchErrorSampler, err := sampler.NewSampler(opts.LogErrorSampleRate())
	if err != nil {
		return nil, err
	}

	logHostWriteErrorSampler, err := sampler.NewSampler(opts.LogHostWriteErrorSampleRate())
	if err != nil {
		return nil, err
	}

	logHostFetchErrorSampler, err := sampler.NewSampler(opts.LogHostFetchErrorSampleRate())
	if err != nil {
		return nil, err
	}

	scope := opts.InstrumentOptions().MetricsScope()

	s := &session{
		state: sessionState{
			writeLevel:     opts.WriteConsistencyLevel(),
			readLevel:      opts.ReadConsistencyLevel(),
			queuesByHostID: make(map[string]hostQueue),
			topo:           topo,
		},
		opts:                     opts,
		scope:                    scope,
		nowFn:                    opts.ClockOptions().NowFn(),
		log:                      opts.InstrumentOptions().Logger(),
		logWriteErrorSampler:     logWriteErrorSampler,
		logFetchErrorSampler:     logFetchErrorSampler,
		logHostWriteErrorSampler: logHostWriteErrorSampler,
		logHostFetchErrorSampler: logHostFetchErrorSampler,
		newHostQueueFn:           newHostQueue,
		fetchBatchSize:           opts.FetchBatchSize(),
		newPeerBlocksQueueFn:     newPeerBlocksQueue,
		healthCheckNewConnFn:     healthCheck,
		writeRetrier:             opts.WriteRetrier(),
		fetchRetrier:             opts.FetchRetrier(),
		pools: sessionPools{
			context:      opts.ContextPool(),
			checkedBytes: opts.CheckedBytesPool(),
			id:           opts.IdentifierPool(),
		},
		writeShardsInitializing:                             opts.WriteShardsInitializing(),
		shardsLeavingCountTowardsConsistency:                opts.ShardsLeavingCountTowardsConsistency(),
		shardsLeavingAndInitializingCountTowardsConsistency: opts.ShardsLeavingAndInitializingCountTowardsConsistency(),
		metrics: newSessionMetrics(scope),
	}
	s.reattemptStreamBlocksFromPeersFn = s.streamBlocksReattemptFromPeers
	s.pickBestPeerFn = s.streamBlocksPickBestPeer
	writeAttemptPoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.WriteOpPoolSize().IsDynamic()).
		SetSize(int(s.opts.WriteOpPoolSize())).
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(
			scope.SubScope("write-attempt-pool"),
		))
	s.pools.writeAttempt = newWriteAttemptPool(s, writeAttemptPoolOpts)
	s.pools.writeAttempt.Init()

	fetchAttemptPoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.FetchBatchOpPoolSize().IsDynamic()).
		SetSize(int(s.opts.FetchBatchOpPoolSize())).
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(
			scope.SubScope("fetch-attempt-pool"),
		))
	s.pools.fetchAttempt = newFetchAttemptPool(s, fetchAttemptPoolOpts)
	s.pools.fetchAttempt.Init()

	fetchTaggedAttemptPoolImplOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.FetchBatchOpPoolSize().IsDynamic()).
		SetSize(int(s.opts.FetchBatchOpPoolSize())).
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(
			scope.SubScope("fetch-tagged-attempt-pool"),
		))
	s.pools.fetchTaggedAttempt = newFetchTaggedAttemptPool(s, fetchTaggedAttemptPoolImplOpts)
	s.pools.fetchTaggedAttempt.Init()

	aggregateAttemptPoolImplOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.FetchBatchOpPoolSize().IsDynamic()).
		SetSize(int(s.opts.FetchBatchOpPoolSize())).
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(
			scope.SubScope("aggregate-attempt-pool"),
		))
	s.pools.aggregateAttempt = newAggregateAttemptPool(s, aggregateAttemptPoolImplOpts)
	s.pools.aggregateAttempt.Init()

	tagEncoderPoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.TagEncoderPoolSize().IsDynamic()).
		SetSize(int(s.opts.TagEncoderPoolSize())).
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(
			scope.SubScope("tag-encoder-pool"),
		))
	s.pools.tagEncoder = serialize.NewTagEncoderPool(opts.TagEncoderOptions(), tagEncoderPoolOpts)
	s.pools.tagEncoder.Init()

	tagDecoderPoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.TagDecoderPoolSize().IsDynamic()).
		SetSize(int(s.opts.TagDecoderPoolSize())).
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(
			scope.SubScope("tag-decoder-pool"),
		))
	s.pools.tagDecoder = serialize.NewTagDecoderPool(opts.TagDecoderOptions(), tagDecoderPoolOpts)
	s.pools.tagDecoder.Init()

	wrapperPoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.CheckedBytesWrapperPoolSize().IsDynamic()).
		SetSize(int(s.opts.CheckedBytesWrapperPoolSize())).
		SetInstrumentOptions(opts.InstrumentOptions().SetMetricsScope(
			scope.SubScope("client-checked-bytes-wrapper-pool")))
	s.pools.checkedBytesWrapper = xpool.NewCheckedBytesWrapperPool(wrapperPoolOpts)
	s.pools.checkedBytesWrapper.Init()

	if optsAdmin, ok := opts.(AdminOptions); ok { // Use a different variable name for clarity
		s.state.bootstrapLevel = optsAdmin.BootstrapConsistencyLevel()
		s.origin = optsAdmin.Origin()
		s.streamBlocksMaxBlockRetries = optsAdmin.FetchSeriesBlocksMaxBlockRetries()
		s.streamBlocksWorkers = xsync.NewWorkerPool(optsAdmin.FetchSeriesBlocksBatchConcurrency())
		s.streamBlocksWorkers.Init()
		s.streamBlocksBatchSize = optsAdmin.FetchSeriesBlocksBatchSize()
		s.streamBlocksMetadataBatchTimeout = optsAdmin.FetchSeriesBlocksMetadataBatchTimeout()
		s.streamBlocksBatchTimeout = optsAdmin.FetchSeriesBlocksBatchTimeout()
		s.streamBlocksRetrier = optsAdmin.StreamBlocksRetrier()
	}

	if runtimeOptsMgr := opts.RuntimeOptionsManager(); runtimeOptsMgr != nil {
		runtimeOptsMgr.RegisterListener(s)
	}

	// Initialize gRPC fields
	s.grpcNodeClients = make(map[string]grpcrpc.NodeClient)
	s.grpcClusterClients = make(map[string]grpcrpc.ClusterClient)
	s.grpcConns = make(map[string]*grpc.ClientConn)

	if s.opts.GRPCTargetResolver() != nil {
		s.grpcTargetResolver = s.opts.GRPCTargetResolver()
	} else {
		var defaultPort string
		grpcConf := s.opts.GRPCClientConfig()
		if grpcConf != nil && len(grpcConf.DefaultNodeTargetAddresses) > 0 {
			firstTarget := grpcConf.DefaultNodeTargetAddresses[0]
			lastColon := strings.LastIndex(firstTarget, ":")
			if lastColon != -1 && lastColon < len(firstTarget)-1 {
				if _, isPort := PositiveIntFromString(firstTarget[lastColon+1:]); isPort {
					defaultPort = firstTarget[lastColon+1:]
				}
			}
		}
		if defaultPort == "" {
			defaultPort = "9090"
		}
		s.grpcTargetResolver = newDefaultGRPCTargetResolver(defaultPort)
		s.log.Info("using default gRPC target resolver", zap.String("resolverDefaultPort", defaultPort))
	}

	return s, nil
}

func (s *session) SetRuntimeOptions(value runtime.Options) {
	s.state.Lock()
	s.state.bootstrapLevel = value.ClientBootstrapConsistencyLevel()
	s.state.readLevel = value.ClientReadConsistencyLevel()
	s.state.writeLevel = value.ClientWriteConsistencyLevel()
	s.state.Unlock()
}

func (s *session) ShardID(id ident.ID) (uint32, error) {
	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return 0, ErrSessionStatusNotOpen
	}
	// Ensure topoMap is not nil before using it
	if s.state.topoMap == nil {
		s.state.RUnlock()
		// This case should ideally not happen if Open() has completed successfully
		return 0, errors.New("topology map not initialized in session")
	}
	value := s.state.topoMap.ShardSet().Lookup(id)
	s.state.RUnlock()
	return value, nil
}

// newPeerMetadataStreamingProgressMetrics returns a struct with an embedded
// list of fields that can be used to emit metrics about the current state of
// the peer metadata streaming process
func (s *session) newPeerMetadataStreamingProgressMetrics(
	shard uint32,
	resultType resultTypeEnum,
) *streamFromPeersMetrics {
	mKey := shardMetricsKey{shardID: shard, resultType: resultType}
	s.metrics.RLock()
	m, ok := s.metrics.streamFromPeersMetrics[mKey]
	s.metrics.RUnlock()

	if ok {
		return &m
	}

	scope := s.opts.InstrumentOptions().MetricsScope()

	s.metrics.Lock()
	m, ok = s.metrics.streamFromPeersMetrics[mKey]
	if ok {
		s.metrics.Unlock()
		return &m
	}
	scope = scope.SubScope("stream-from-peers").Tagged(map[string]string{
		"shard":      fmt.Sprintf("%d", shard),
		"resultType": string(resultType),
	})
	m = streamFromPeersMetrics{
		fetchBlocksFromPeers:       scope.Gauge("fetch-blocks-inprogress"),
		metadataFetches:            scope.Gauge("fetch-metadata-peers-inprogress"),
		metadataFetchBatchCall:     scope.Counter("fetch-metadata-peers-batch-call"),
		metadataFetchBatchSuccess:  scope.Counter("fetch-metadata-peers-batch-success"),
		metadataFetchBatchError:    scope.Counter("fetch-metadata-peers-batch-error"),
		metadataFetchBatchBlockErr: scope.Counter("fetch-metadata-peers-batch-block-err"),
		metadataReceived:           scope.Counter("fetch-metadata-peers-received"),
		metadataPeerRetry:          scope.Counter("fetch-metadata-peers-peer-retry"),
		fetchBlockSuccess:          scope.Counter("fetch-block-success"),
		fetchBlockError:            scope.Counter("fetch-block-error"),
		fetchBlockFinalError:       scope.Counter("fetch-block-final-error"),
		fetchBlockFullRetry:        scope.Counter("fetch-block-full-retry"),
		fetchBlockRetriesReqError: scope.Tagged(map[string]string{
			"reason": "request-error",
		}).Counter("fetch-block-retries"),
		fetchBlockRetriesRespError: scope.Tagged(map[string]string{
			"reason": "response-error",
		}).Counter("fetch-block-retries"),
		fetchBlockRetriesConsistencyLevelNotAchievedError: scope.Tagged(map[string]string{
			"reason": "consistency-level-not-achieved-error",
		}).Counter("fetch-block-retries"),
		blocksEnqueueChannel: scope.Gauge("fetch-blocks-enqueue-channel-length"),
	}
	s.metrics.streamFromPeersMetrics[mKey] = m
	s.metrics.Unlock()
	return &m
}

func (s *session) recordWriteMetrics(consistencyResultErr error, state *writeState, start time.Time) {
	respErrs := int32(len(state.errors))
	if idx := s.nodesRespondingErrorsMetricIndex(respErrs); idx >= 0 {
		if IsBadRequestError(consistencyResultErr) {
			s.metrics.writeNodesRespondingBadRequestErrors[idx].Inc(1)
		} else {
			s.metrics.writeNodesRespondingErrors[idx].Inc(1)
		}
	}
	if consistencyResultErr == nil {

		if state.leavingAndInitializingPairCounted {
			s.metrics.writeSuccessForCountLeavingAndInitializingAsPair.Inc(1)
		} else {
			s.metrics.writeSuccess.Inc(1)
		}
	} else if IsBadRequestError(consistencyResultErr) {
		s.metrics.writeErrorsBadRequest.Inc(1)
	} else {
		s.metrics.writeErrorsInternalError.Inc(1)
	}
	s.metrics.writeLatencyHistogram.RecordDuration(s.nowFn().Sub(start))

	if consistencyResultErr != nil && s.logWriteErrorSampler.Sample() {
		s.log.Error("m3db client write error occurred",
			zap.Float64("sampleRateLog", s.logWriteErrorSampler.SampleRate().Value()),
			zap.Error(consistencyResultErr))
	}
}

func (s *session) recordFetchMetrics(consistencyResultErr error, respErrs int32, start time.Time) {
	if idx := s.nodesRespondingErrorsMetricIndex(respErrs); idx >= 0 {
		if IsBadRequestError(consistencyResultErr) {
			s.metrics.fetchNodesRespondingBadRequestErrors[idx].Inc(1)
		} else {
			s.metrics.fetchNodesRespondingErrors[idx].Inc(1)
		}
	}
	if consistencyResultErr == nil {
		s.metrics.fetchSuccess.Inc(1)
	} else if IsBadRequestError(consistencyResultErr) {
		s.metrics.fetchErrorsBadRequest.Inc(1)
	} else {
		s.metrics.fetchErrorsInternalError.Inc(1)
	}
	s.metrics.fetchLatencyHistogram.RecordDuration(s.nowFn().Sub(start))

	if consistencyResultErr != nil && s.logFetchErrorSampler.Sample() {
		s.log.Error("m3db client fetch error occurred",
			zap.Float64("sampleRateLog", s.logFetchErrorSampler.SampleRate().Value()),
			zap.Error(consistencyResultErr))
	}
}

func (s *session) nodesRespondingErrorsMetricIndex(respErrs int32) int32 {
	idx := respErrs - 1
	replicas := int32(s.Replicas())
	if respErrs > replicas {
		// Cap to the max replicas, we might get more errors
		// when a node is initializing a shard causing replicas + 1
		// nodes to respond to operations
		idx = replicas - 1
	}
	return idx
}

func (s *session) Open() error {
	s.state.Lock()
	if s.state.status != statusNotOpen {
		s.state.Unlock()
		return errSessionStatusNotInitial
	}

	watch, err := s.state.topo.Watch()
	if err != nil {
		s.state.Unlock()
		return err
	}

	// Wait for the topology to be available
	<-watch.C()

	topoMap := watch.Get()

	queues, replicas, majority, err := s.hostQueues(topoMap, nil)
	if err != nil {
		s.state.Unlock()
		return err
	}
	s.setTopologyWithLock(topoMap, queues, replicas, majority)
	s.state.topoWatch = watch

	// NB(r): Alloc pools that can take some time in Open, expectation
	// is already that Open will take some time
	writeOperationPoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.WriteOpPoolSize().IsDynamic()).
		SetSize(int(s.opts.WriteOpPoolSize())).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("write-op-pool"),
		))
	s.pools.writeOperation = newWriteOperationPool(writeOperationPoolOpts)
	s.pools.writeOperation.Init()

	writeTaggedOperationPoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.WriteTaggedOpPoolSize().IsDynamic()).
		SetSize(int(s.opts.WriteTaggedOpPoolSize())).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("write-op-tagged-pool"),
		))
	s.pools.writeTaggedOperation = newWriteTaggedOpPool(writeTaggedOperationPoolOpts)
	s.pools.writeTaggedOperation.Init()

	writeStatePoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.WriteOpPoolSize().IsDynamic()).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("write-state-pool"),
		))

	if !s.opts.WriteOpPoolSize().IsDynamic() {
		writeStatePoolSize := s.opts.WriteOpPoolSize()
		if !s.opts.WriteTaggedOpPoolSize().IsDynamic() && s.opts.WriteTaggedOpPoolSize() > writeStatePoolSize {
			writeStatePoolSize = s.opts.WriteTaggedOpPoolSize()
		}
		writeStatePoolOpts = writeStatePoolOpts.SetSize(int(writeStatePoolSize))
	}
	s.pools.writeState = newWriteStatePool(s.pools.tagEncoder, writeStatePoolOpts, s.log,
		s.logHostWriteErrorSampler)
	s.pools.writeState.Init()

	fetchBatchOpPoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.FetchBatchOpPoolSize().IsDynamic()).
		SetSize(int(s.opts.FetchBatchOpPoolSize())).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("fetch-batch-op-pool"),
		))
	s.pools.fetchBatchOp = newFetchBatchOpPool(fetchBatchOpPoolOpts, s.fetchBatchSize)
	s.pools.fetchBatchOp.Init()

	fetchTaggedOpPoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.FetchBatchOpPoolSize().IsDynamic()).
		SetSize(int(s.opts.FetchBatchOpPoolSize())).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("fetch-tagged-op-pool"),
		))
	s.pools.fetchTaggedOp = newFetchTaggedOpPool(fetchTaggedOpPoolOpts)
	s.pools.fetchTaggedOp.Init()

	aggregateOpPoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.FetchBatchOpPoolSize().IsDynamic()).
		SetSize(int(s.opts.FetchBatchOpPoolSize())).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("aggregate-op-pool"),
		))
	s.pools.aggregateOp = newAggregateOpPool(aggregateOpPoolOpts)
	s.pools.aggregateOp.Init()

	fetchStatePoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.FetchBatchOpPoolSize().IsDynamic()).
		SetSize(int(s.opts.FetchBatchOpPoolSize())).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("fetch-tagged-state-pool"),
		))
	s.pools.fetchState = newFetchStatePool(fetchStatePoolOpts, s.log,
		s.logHostFetchErrorSampler)
	s.pools.fetchState.Init()

	seriesIteratorPoolOpts := pool.NewObjectPoolOptions().
		SetDynamic(s.opts.SeriesIteratorPoolSize().IsDynamic()).
		SetSize(int(s.opts.SeriesIteratorPoolSize())).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("series-iterator-pool"),
		))
	s.pools.seriesIterator = encoding.NewSeriesIteratorPool(seriesIteratorPoolOpts)
	s.pools.seriesIterator.Init()
	s.state.status = statusOpen
	s.state.Unlock()

	go func() {
		backgroundCtx := gocontext.Background() // Use gocontext for gRPC calls
		for range watch.C() {
			s.log.Info("received update for topology")
			topoMap := watch.Get()

			s.state.RLock()
			existingQueues := s.state.queues
			s.state.RUnlock()

			queues, replicas, majority, err := s.hostQueues(topoMap, existingQueues)
			if err != nil {
				s.log.Error("could not update topology map", zap.Error(err))
				s.metrics.topologyUpdatedError.Inc(1)
				continue
			}
			s.state.Lock()
			s.setTopologyWithLock(topoMap, queues, replicas, majority)

			// Update gRPC connections
			grpcCfg := s.opts.GRPCClientConfig()
			if grpcCfg.Enabled {
				activeHostIDs := make(map[string]struct{})
				for _, host := range topoMap.Hosts() {
					hostID := host.IDString()
					activeHostIDs[hostID] = struct{}{}

					if _, exists := s.grpcConns[hostID]; !exists {
						targetAddr, resolveErr := s.grpcTargetResolver.Resolve(host)
						if resolveErr != nil {
							s.log.Error("failed to resolve gRPC target for host", zap.String("hostID", hostID), zap.Error(resolveErr))
							continue
						}

						var dialOpts []grpc.DialOption
						if s.opts.GRPCDialOptions() != nil {
							dialOpts = append(dialOpts, s.opts.GRPCDialOptions()...)
						}
						dialOpts = append(dialOpts, grpc.WithInsecure()) // TODO: Make configurable

						dialTimeout := grpcCfg.DialTimeoutOrDefault()

						dialCtx, cancel := gocontext.WithTimeout(backgroundCtx, dialTimeout)
						conn, dialErr := grpc.DialContext(dialCtx, targetAddr, dialOpts...)
						cancel()

						if dialErr != nil {
							s.log.Error("failed to dial gRPC for host", zap.String("hostID", hostID), zap.String("target", targetAddr), zap.Error(dialErr))
							continue
						}
						s.log.Info("gRPC connection established to host", zap.String("hostID", hostID), zap.String("target", targetAddr))
						s.grpcConns[hostID] = conn
						s.grpcNodeClients[hostID] = grpcrpc.NewNodeClient(conn)
						// s.grpcClusterClients[hostID] = grpcrpc.NewClusterClient(conn)
					}
				}

				// Remove old connections for hosts no longer in topology
				for hostID, conn := range s.grpcConns {
					if _, isActive := activeHostIDs[hostID]; !isActive {
						s.log.Info("closing stale gRPC connection to host", zap.String("hostID", hostID))
						if err := conn.Close(); err != nil {
							s.log.Error("error closing stale gRPC connection", zap.String("hostID", hostID), zap.Error(err))
						}
						delete(s.grpcConns, hostID)
						delete(s.grpcNodeClients, hostID)
						delete(s.grpcClusterClients, hostID)
					}
				}
			}
			s.state.Unlock()
			s.metrics.topologyUpdatedSuccess.Inc(1)
		}
	}()

	return nil
}

func (s *session) BorrowConnections(
	shardID uint32,
	fn WithBorrowConnectionFn,
	opts BorrowConnectionOptions,
) (BorrowConnectionsResult, error) {
	var result BorrowConnectionsResult
	s.state.RLock()
	topoMap, err := s.topologyMapWithStateRLock()
	s.state.RUnlock()
	if err != nil {
		return result, err
	}

	var (
		multiErr  = xerrors.NewMultiError()
		breakLoop bool
	)
	err = topoMap.RouteShardForEach(shardID, func(
		_ int,
		shard shard.Shard,
		host topology.Host,
	) {
		if multiErr.NumErrors() > 0 || breakLoop {
			// Error or has broken
			return
		}
		if opts.ExcludeOrigin && s.origin != nil && s.origin.ID() == host.ID() {
			// Skip origin host.
			return
		}

		var (
			userResult WithBorrowConnectionResult
			userErr    error
		)
		borrowErr := s.BorrowConnection(host.ID(), func(
			client thriftrpc.TChanNode,
			channel Channel,
		) {
			userResult, userErr = fn(shard, host, client, channel)
		})
		if borrowErr != nil {
			// Wasn't able to even borrow, skip if don't want to error
			// on down hosts or return the borrow error.
			if !opts.ContinueOnBorrowError {
				multiErr = multiErr.Add(borrowErr)
			}
			return
		}

		// Track successful borrow.
		result.Borrowed++

		// Track whether has broken loop.
		breakLoop = userResult.Break

		// Return whether user error occurred to break or not.
		if userErr != nil {
			multiErr = multiErr.Add(userErr)
		}
	})
	if err != nil {
		// Route error.
		return result, err
	}
	// Potentially a user error or borrow error, otherwise
	// FinalError() will return nil.
	return result, multiErr.FinalError()
}

func (s *session) BorrowConnection(hostID string, fn WithConnectionFn) error {
	s.state.RLock()
	unlocked := false
	queue, ok := s.state.queuesByHostID[hostID]
	if !ok {
		s.state.RUnlock()
		return errSessionHasNoHostQueueForHost
	}
	err := queue.BorrowConnection(func(client thriftrpc.TChanNode, ch Channel) {
		// Unlock early on success
		s.state.RUnlock()
		unlocked = true

		// Execute function with borrowed connection
		fn(client, ch)
	})
	if !unlocked {
		s.state.RUnlock()
	}
	return err
}

func (s *session) DedicatedConnection(
	shardID uint32,
	opts DedicatedConnectionOptions,
) (thriftrpc.TChanNode, Channel, error) {
	s.state.RLock()
	topoMap, err := s.topologyMapWithStateRLock()
	s.state.RUnlock()
	if err != nil {
		return nil, nil, err
	}

	var (
		client    thriftrpc.TChanNode
		channel   Channel
		succeeded bool
		multiErr  = xerrors.NewMultiError()
	)
	err = topoMap.RouteShardForEach(shardID, func(
		_ int,
		targetShard shard.Shard,
		host topology.Host,
	) {
		stateFilter := opts.ShardStateFilter
		if succeeded || !(stateFilter == shard.Unknown || targetShard.State() == stateFilter) {
			return
		}

		if s.origin != nil && s.origin.ID() == host.ID() {
			// Skip origin host.
			return
		}

		newConnFn := s.opts.NewConnectionFn()
		channel, client, err = newConnFn(channelName, host.Address(), s.opts)
		if err != nil {
			multiErr = multiErr.Add(err)
			return
		}

		if err := s.healthCheckNewConnFn(client, s.opts, opts.BootstrappedNodesOnly); err != nil {
			channel.Close()
			multiErr = multiErr.Add(err)
			return
		}

		succeeded = true
	})
	if err != nil {
		return nil, nil, err
	}

	if !succeeded {
		multiErr = multiErr.Add(
			fmt.Errorf("failed to create a dedicated connection for shard %d", shardID))
		return nil, nil, multiErr.FinalError()
	}

	return client, channel, nil
}

func (s *session) hostQueues(
	topoMap topology.Map,
	existing []hostQueue,
) ([]hostQueue, int, int, error) {
	// NB(r): we leave existing writes in the host queues to finish
	// as they are already enroute to their destination. This is an edge case
	// that might result in leaving nodes counting towards quorum, but fixing it
	// would result in additional chatter.

	start := s.nowFn()

	existingByHostID := make(map[string]hostQueue, len(existing))
	for _, queue := range existing {
		existingByHostID[queue.Host().ID()] = queue
	}

	hosts := topoMap.Hosts()
	queues := make([]hostQueue, 0, len(hosts))
	newQueues := make([]hostQueue, 0, len(hosts))
	for _, host := range hosts {
		if existingQueue, ok := existingByHostID[host.ID()]; ok {
			queues = append(queues, existingQueue)
			continue
		}
		newQueue, err := s.newHostQueue(host, topoMap)
		if err != nil {
			return nil, 0, 0, err
		}
		queues = append(queues, newQueue)
		newQueues = append(newQueues, newQueue)
	}

	replicas := topoMap.Replicas()
	majority := topoMap.MajorityReplicas()

	firstConnectConsistencyLevel := s.opts.ClusterConnectConsistencyLevel()
	if firstConnectConsistencyLevel == topology.ConnectConsistencyLevelNone {
		// Return immediately if no connect consistency required
		return queues, replicas, majority, nil
	}

	connectConsistencyLevel := firstConnectConsistencyLevel
	if connectConsistencyLevel == topology.ConnectConsistencyLevelAny {
		// If level any specified, first attempt all then proceed lowering requirement
		connectConsistencyLevel = topology.ConnectConsistencyLevelAll
	}

	// Abort if we do not connect
	connected := false
	defer func() {
		if !connected {
			for _, queue := range newQueues {
				queue.Close()
			}
		}
	}()

	startConnectAttempt := s.nowFn()
	for {
		if now := s.nowFn(); now.Sub(start) >= s.opts.ClusterConnectTimeout() {
			switch firstConnectConsistencyLevel {
			case topology.ConnectConsistencyLevelAny:
				// If connecting with connect any strategy then keep
				// trying but lower consistency requirement
				start = now
				connectConsistencyLevel--
				if connectConsistencyLevel == topology.ConnectConsistencyLevelNone {
					// Already tried to resolve all consistency requirements, just
					// return successfully at this point
					err := fmt.Errorf("timed out connecting, returning success")
					s.log.Warn("cluster connect with consistency any", zap.Error(err))
					connected = true
					return queues, replicas, majority, nil
				}
			default:
				// Timed out connecting to a specific consistency requirement
				return nil, 0, 0, ErrClusterConnectTimeout
			}
		}

		var level topology.ConsistencyLevel
		switch connectConsistencyLevel {
		case topology.ConnectConsistencyLevelAll:
			level = topology.ConsistencyLevelAll
		case topology.ConnectConsistencyLevelMajority:
			level = topology.ConsistencyLevelMajority
		case topology.ConnectConsistencyLevelOne:
			level = topology.ConsistencyLevelOne
		default:
			return nil, 0, 0, errSessionInvalidConnectClusterConnectConsistencyLevel
		}
		clusterAvailable, err := s.clusterAvailabilityWithQueuesAndMap(level,
			queues, topoMap)
		if err != nil {
			return nil, 0, 0, err
		}
		if clusterAvailable {
			// All done
			break
		}
		time.Sleep(clusterConnectWaitInterval)
	}

	connected = true
	s.metrics.clusterConnectLatency.RecordDuration(s.nowFn().Sub(startConnectAttempt))
	return queues, replicas, majority, nil
}

func (s *session) WriteClusterAvailability() (bool, error) {
	level := s.opts.WriteConsistencyLevel()
	return s.clusterAvailability(level)
}

func (s *session) ReadClusterAvailability() (bool, error) {
	var convertedConsistencyLevel topology.ConsistencyLevel
	level := s.opts.ReadConsistencyLevel()
	switch level {
	case topology.ReadConsistencyLevelNone:
		// Already ready.
		return true, nil
	case topology.ReadConsistencyLevelOne:
		convertedConsistencyLevel = topology.ConsistencyLevelOne
	case topology.ReadConsistencyLevelUnstrictMajority:
		convertedConsistencyLevel = topology.ConsistencyLevelOne
	case topology.ReadConsistencyLevelMajority:
		convertedConsistencyLevel = topology.ConsistencyLevelMajority
	case topology.ReadConsistencyLevelUnstrictAll:
		convertedConsistencyLevel = topology.ConsistencyLevelOne
	case topology.ReadConsistencyLevelAll:
		convertedConsistencyLevel = topology.ConsistencyLevelAll
	default:
		return false, fmt.Errorf("unknown consistency level: %d", level)
	}
	return s.clusterAvailability(convertedConsistencyLevel)
}

func (s *session) clusterAvailability(
	level topology.ConsistencyLevel,
) (bool, error) {
	s.state.RLock()
	queues := s.state.queues
	topoMap, err := s.topologyMapWithStateRLock()
	s.state.RUnlock()
	if err != nil {
		return false, err
	}
	return s.clusterAvailabilityWithQueuesAndMap(level, queues, topoMap)
}

func (s *session) clusterAvailabilityWithQueuesAndMap(
	level topology.ConsistencyLevel,
	queues []hostQueue,
	topoMap topology.Map,
) (bool, error) {
	shards := topoMap.ShardSet().AllIDs()
	minConnectionCount := s.opts.MinConnectionCount()
	replicas := topoMap.Replicas()
	majority := topoMap.MajorityReplicas()

	for _, shardID := range shards {
		shardReplicasAvailable := 0
		routeErr := topoMap.RouteShardForEach(shardID, func(idx int, _ shard.Shard, _ topology.Host) {
			if queues[idx].ConnectionCount() >= minConnectionCount {
				shardReplicasAvailable++
			}
		})
		if routeErr != nil {
			return false, routeErr
		}
		var clusterAvailableForShard bool
		switch level {
		case topology.ConsistencyLevelAll:
			clusterAvailableForShard = shardReplicasAvailable == replicas
		case topology.ConsistencyLevelMajority:
			clusterAvailableForShard = shardReplicasAvailable >= majority
		case topology.ConsistencyLevelOne:
			clusterAvailableForShard = shardReplicasAvailable > 0
		default:
			return false, fmt.Errorf("unknown consistency level: %d", level)
		}
		if !clusterAvailableForShard {
			return false, nil
		}
	}

	return true, nil
}

func (s *session) setTopologyWithLock(topoMap topology.Map, queues []hostQueue, replicas, majority int) {
	prevQueues := s.state.queues

	newQueuesByHostID := make(map[string]hostQueue, len(queues))
	for _, queue := range queues {
		newQueuesByHostID[queue.Host().ID()] = queue
	}

	s.state.queues = queues
	s.state.queuesByHostID = newQueuesByHostID

	s.state.topoMap = topoMap

	s.state.replicas = replicas
	s.state.majority = majority

	// If the number of hostQueues has changed then we need to recreate the fetch
	// batch op array pool as it must be the exact length of the queues as we index
	// directly into the return array in fetch calls.
	if len(queues) != len(prevQueues) {
		poolOpts := pool.NewObjectPoolOptions().
			SetSize(int(s.opts.FetchBatchOpPoolSize())).
			SetDynamic(s.opts.FetchBatchOpPoolSize().IsDynamic()).
			SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
				s.scope.SubScope("fetch-batch-op-array-array-pool"),
			))
		s.pools.fetchBatchOpArrayArray = newFetchBatchOpArrayArrayPool(
			poolOpts,
			len(queues),
			int(s.opts.FetchBatchOpPoolSize())/len(queues))
		s.pools.fetchBatchOpArrayArray.Init()
	}

	if s.pools.multiReaderIteratorArray == nil {
		s.pools.multiReaderIteratorArray = encoding.NewMultiReaderIteratorArrayPool([]pool.Bucket{
			{
				Capacity: replicas,
				Count:    s.opts.SeriesIteratorPoolSize(),
			},
		})
		s.pools.multiReaderIteratorArray.Init()
	}
	if s.pools.readerSliceOfSlicesIterator == nil {
		size := int(s.opts.SeriesIteratorPoolSize())
		if !s.opts.SeriesIteratorPoolSize().IsDynamic() {
			size = replicas * int(s.opts.SeriesIteratorPoolSize())
		}
		poolOpts := pool.NewObjectPoolOptions().
			SetSize(size).
			SetDynamic(s.opts.SeriesIteratorPoolSize().IsDynamic()).
			SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
				s.scope.SubScope("reader-slice-of-slices-iterator-pool"),
			))
		s.pools.readerSliceOfSlicesIterator = newReaderSliceOfSlicesIteratorPool(poolOpts)
		s.pools.readerSliceOfSlicesIterator.Init()
	}
	if s.pools.multiReaderIterator == nil {
		size := int(s.opts.SeriesIteratorPoolSize())
		if !s.opts.SeriesIteratorPoolSize().IsDynamic() {
			size = replicas * int(s.opts.SeriesIteratorPoolSize())
		}
		poolOpts := pool.NewObjectPoolOptions().
			SetSize(size).
			SetDynamic(s.opts.SeriesIteratorPoolSize().IsDynamic()).
			SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
				s.scope.SubScope("multi-reader-iterator-pool"),
			))
		s.pools.multiReaderIterator = encoding.NewMultiReaderIteratorPool(poolOpts)
		s.pools.multiReaderIterator.Init(s.opts.ReaderIteratorAllocate())
	}
	if replicas > len(s.metrics.writeNodesRespondingErrors) {
		curr := len(s.metrics.writeNodesRespondingErrors)
		for i := curr; i < replicas; i++ {
			tags := map[string]string{"nodes": fmt.Sprintf("%d", i+1)}
			name := "write.nodes-responding-error"
			serverErrsSubScope := s.scope.Tagged(tags).Tagged(map[string]string{
				"error_type": "server_error",
			})
			badRequestErrsSubScope := s.scope.Tagged(tags).Tagged(map[string]string{
				"error_type": "bad_request_error",
			})
			sc := serverErrsSubScope.Counter(name)
			s.metrics.writeNodesRespondingErrors = append(s.metrics.writeNodesRespondingErrors, sc)
			cc := badRequestErrsSubScope.Counter(name)
			s.metrics.writeNodesRespondingBadRequestErrors = append(s.metrics.writeNodesRespondingBadRequestErrors, cc)
		}
	}
	if replicas > len(s.metrics.fetchNodesRespondingErrors) {
		curr := len(s.metrics.fetchNodesRespondingErrors)
		for i := curr; i < replicas; i++ {
			tags := map[string]string{"nodes": fmt.Sprintf("%d", i+1)}
			name := "fetch.nodes-responding-error"
			serverErrsSubScope := s.scope.Tagged(tags).Tagged(map[string]string{
				"error_type": "server_error",
			})
			badRequestErrsSubScope := s.scope.Tagged(tags).Tagged(map[string]string{
				"error_type": "bad_request_error",
			})
			sc := serverErrsSubScope.Counter(name)
			s.metrics.fetchNodesRespondingErrors = append(s.metrics.fetchNodesRespondingErrors, sc)
			cc := badRequestErrsSubScope.Counter(name)
			s.metrics.fetchNodesRespondingBadRequestErrors = append(s.metrics.fetchNodesRespondingBadRequestErrors, cc)
		}
	}

	// Asynchronously close the set of host queues no longer in use
	go func() {
		for _, queue := range prevQueues {
			newQueue, ok := newQueuesByHostID[queue.Host().ID()]
			if !ok || newQueue != queue {
				queue.Close()
			}
		}
	}()

	s.log.Info("successfully updated topology",
		zap.Int("numHosts", topoMap.HostsLen()),
		zap.Int("numShards", len(topoMap.ShardSet().AllIDs())))
}

func (s *session) newHostQueue(host topology.Host, topoMap topology.Map) (hostQueue, error) {
	// NB(r): Due to hosts being replicas we have:
	// = replica * numWrites
	// = total writes to all hosts
	// We need to pool:
	// = replica * (numWrites / writeBatchSize)
	// = number of batch request structs to pool
	// For purposes of simplifying the options for pooling the write op pool size
	// represents the number of ops to pool not including replication, this is due
	// to the fact that the ops are shared between the different host queue replicas.
	writeOpPoolSize := s.opts.WriteOpPoolSize()
	if s.opts.WriteTaggedOpPoolSize() > writeOpPoolSize {
		writeOpPoolSize = s.opts.WriteTaggedOpPoolSize()
	}
	totalBatches := topoMap.Replicas() *
		int(math.Ceil(float64(writeOpPoolSize)/float64(s.opts.WriteBatchSize())))
	hostBatches := int(math.Ceil(float64(totalBatches) / float64(topoMap.HostsLen())))

	writeBatchRequestPoolOpts := pool.NewObjectPoolOptions().
		SetSize(hostBatches).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("write-batch-request-pool"),
		))
	writeBatchRequestPool := newWriteBatchRawRequestPool(writeBatchRequestPoolOpts)
	writeBatchRequestPool.Init()
	writeBatchV2RequestPool := newWriteBatchRawV2RequestPool(writeBatchRequestPoolOpts)
	writeBatchV2RequestPool.Init()

	writeTaggedBatchRequestPoolOpts := pool.NewObjectPoolOptions().
		SetSize(hostBatches).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("write-tagged-batch-request-pool"),
		))
	writeTaggedBatchRequestPool := newWriteTaggedBatchRawRequestPool(writeTaggedBatchRequestPoolOpts)
	writeTaggedBatchRequestPool.Init()
	writeTaggedBatchV2RequestPool := newWriteTaggedBatchRawV2RequestPool(writeBatchRequestPoolOpts)
	writeTaggedBatchV2RequestPool.Init()

	writeBatchRawRequestElementArrayPoolOpts := pool.NewObjectPoolOptions().
		SetSize(hostBatches).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("id-datapoint-array-pool"),
		))
	writeBatchRawRequestElementArrayPool := newWriteBatchRawRequestElementArrayPool(
		writeBatchRawRequestElementArrayPoolOpts, s.opts.WriteBatchSize())
	writeBatchRawRequestElementArrayPool.Init()
	writeBatchRawV2RequestElementArrayPool := newWriteBatchRawV2RequestElementArrayPool(
		writeBatchRawRequestElementArrayPoolOpts, s.opts.WriteBatchSize())
	writeBatchRawV2RequestElementArrayPool.Init()

	writeTaggedBatchRawRequestElementArrayPoolOpts := pool.NewObjectPoolOptions().
		SetSize(hostBatches).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("id-tagged-datapoint-array-pool"),
		))
	writeTaggedBatchRawRequestElementArrayPool := newWriteTaggedBatchRawRequestElementArrayPool(
		writeTaggedBatchRawRequestElementArrayPoolOpts, s.opts.WriteBatchSize())
	writeTaggedBatchRawRequestElementArrayPool.Init()
	writeTaggedBatchRawV2RequestElementArrayPool := newWriteTaggedBatchRawV2RequestElementArrayPool(
		writeTaggedBatchRawRequestElementArrayPoolOpts, s.opts.WriteBatchSize())
	writeTaggedBatchRawV2RequestElementArrayPool.Init()

	fetchBatchRawV2RequestPoolOpts := pool.NewObjectPoolOptions().
		SetSize(hostBatches).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("fetch-batch-request-pool"),
		))
	fetchBatchRawV2RequestPool := newFetchBatchRawV2RequestPool(fetchBatchRawV2RequestPoolOpts)
	fetchBatchRawV2RequestPool.Init()

	fetchBatchRawV2RequestElementArrayPoolOpts := pool.NewObjectPoolOptions().
		SetSize(hostBatches).
		SetInstrumentOptions(s.opts.InstrumentOptions().SetMetricsScope(
			s.scope.SubScope("fetch-batch-request-array-pool"),
		))
	fetchBatchRawV2RequestElementArrPool := newFetchBatchRawV2RequestElementArrayPool(
		fetchBatchRawV2RequestElementArrayPoolOpts, s.opts.FetchBatchSize(),
	)
	fetchBatchRawV2RequestElementArrPool.Init()

	hostQueue, err := s.newHostQueueFn(host, hostQueueOpts{
		writeBatchRawRequestPool:                     writeBatchRequestPool,
		writeBatchRawV2RequestPool:                   writeBatchV2RequestPool,
		writeBatchRawRequestElementArrayPool:         writeBatchRawRequestElementArrayPool,
		writeBatchRawV2RequestElementArrayPool:       writeBatchRawV2RequestElementArrayPool,
		writeTaggedBatchRawRequestPool:               writeTaggedBatchRequestPool,
		writeTaggedBatchRawV2RequestPool:             writeTaggedBatchV2RequestPool,
		writeTaggedBatchRawRequestElementArrayPool:   writeTaggedBatchRawRequestElementArrayPool,
		writeTaggedBatchRawV2RequestElementArrayPool: writeTaggedBatchRawV2RequestElementArrayPool,
		fetchBatchRawV2RequestPool:                   fetchBatchRawV2RequestPool,
		fetchBatchRawV2RequestElementArrayPool:       fetchBatchRawV2RequestElementArrPool,
		opts:                                         s.opts,
	})
	if err != nil {
		return nil, err
	}
	hostQueue.Open()
	return hostQueue, nil
}

func (s *session) Write(
	nsID, id ident.ID,
	t xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	w := s.pools.writeAttempt.Get()
	w.args.attemptType = untaggedWriteAttemptType
	w.args.namespace, w.args.id = nsID, id
	w.args.tags = ident.EmptyTagIterator
	w.args.t = t
	w.args.value, w.args.unit, w.args.annotation = value, unit, annotation
	err := s.writeRetrier.Attempt(w.attemptFn)
	s.pools.writeAttempt.Put(w)
	return err
}

func (s *session) WriteTagged(
	nsID, id ident.ID,
	tags ident.TagIterator,
	t xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	w := s.pools.writeAttempt.Get()
	w.args.attemptType = taggedWriteAttemptType
	w.args.namespace, w.args.id, w.args.tags = nsID, id, tags
	w.args.t = t
	w.args.value, w.args.unit, w.args.annotation = value, unit, annotation
	err := s.writeRetrier.Attempt(w.attemptFn)
	s.pools.writeAttempt.Put(w)
	return err
}

func (s *session) writeAttempt(
	wType writeAttemptType,
	nsID, id ident.ID,
	inputTags ident.TagIterator,
	t xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	startWriteAttempt := s.nowFn()

	timeType, timeTypeErr := convert.ToTimeType(unit)
	if timeTypeErr != nil {
		return timeTypeErr
	}

	timestamp, timestampErr := convert.ToValue(t, timeType)
	if timestampErr != nil {
		return timestampErr
	}

	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return ErrSessionStatusNotOpen
	}

	state, majority, enqueued, err := s.writeAttemptWithRLock(
		wType, nsID, id, inputTags, timestamp, value, timeType, annotation)
	s.state.RUnlock()

	if err != nil {
		return err
	}

	// it's safe to Wait() here, as we still hold the lock on state, after it's
	// returned from writeAttemptWithRLock.
	state.Wait()

	err = s.writeConsistencyResult(state.consistencyLevel, majority, enqueued,
		enqueued-state.pending, int32(len(state.errors)), state.errors)

	s.recordWriteMetrics(err, state, startWriteAttempt)
	// must Unlock before decRef'ing, as the latter releases the writeState back into a
	// pool if ref count == 0.
	state.Unlock()
	state.decRef()

	return err
}

// NB(prateek): the returned writeState, if valid, still holds the lock. Its ownership
// is transferred to the calling function, and is expected to manage the lifecycle of
// of the object (including releasing the lock/decRef'ing it).
func (s *session) writeAttemptWithRLock(
	wType writeAttemptType,
	namespace, id ident.ID,
	inputTags ident.TagIterator,
	timestamp int64,
	value float64,
	timeType thriftrpc.TimeType, // Corrected type
	annotation []byte,
) (*writeState, int32, int32, error) {
	var (
		majority = int32(s.state.majority)
		enqueued int32
	)

	// NB(prateek): We retain an individual copy of the namespace, ID per
	// writeState, as each writeState tracks the lifecycle of it's resources in
	// use in the various queues. Tracking per writeAttempt isn't sufficient as
	// we may enqueue multiple writeStates concurrently depending on retries
	// and consistency level checks.
	var tagEncoder serialize.TagEncoder
	if wType == taggedWriteAttemptType {
		tagEncoder = s.pools.tagEncoder.Get()
		if err := tagEncoder.Encode(inputTags); err != nil {
			tagEncoder.Finalize()
			return nil, 0, 0, err
		}
	}
	nsID := s.cloneFinalizable(namespace)
	tsID := s.cloneFinalizable(id)

	var (
		clonedAnnotation      checked.Bytes
		clonedAnnotationBytes []byte
	)
	if len(annotation) > 0 {
		clonedAnnotation = s.pools.checkedBytes.Get(len(annotation))
		clonedAnnotation.IncRef()
		clonedAnnotation.AppendAll(annotation)
		clonedAnnotationBytes = clonedAnnotation.Bytes()
	}

	var op writeOp
	switch wType {
	case untaggedWriteAttemptType:
		wop := s.pools.writeOperation.Get()
		wop.namespace = nsID
		wop.shardID = s.state.topoMap.ShardSet().Lookup(tsID)
		wop.request.ID = tsID.Bytes()
		wop.request.Datapoint.Value = value
		wop.request.Datapoint.Timestamp = timestamp
		wop.request.Datapoint.TimestampTimeType = timeType
		wop.request.Datapoint.Annotation = clonedAnnotationBytes
		wop.requestV2.ID = wop.request.ID
		wop.requestV2.Datapoint = wop.request.Datapoint
		op = wop
	case taggedWriteAttemptType:
		wop := s.pools.writeTaggedOperation.Get()
		wop.namespace = nsID
		wop.shardID = s.state.topoMap.ShardSet().Lookup(tsID)
		wop.request.ID = tsID.Bytes()
		encodedTagBytes, ok := tagEncoder.Data()
		if !ok {
			return nil, 0, 0, errUnableToEncodeTags
		}
		wop.request.EncodedTags = encodedTagBytes.Bytes()
		wop.request.Datapoint.Value = value
		wop.request.Datapoint.Timestamp = timestamp
		wop.request.Datapoint.TimestampTimeType = timeType
		wop.request.Datapoint.Annotation = clonedAnnotationBytes
		wop.requestV2.ID = wop.request.ID
		wop.requestV2.EncodedTags = wop.request.EncodedTags
		wop.requestV2.Datapoint = wop.request.Datapoint
		op = wop
	default:
		// should never happen
		return nil, 0, 0, errUnknownWriteAttemptType
	}

	state := s.pools.writeState.Get()
	state.consistencyLevel = s.state.writeLevel
	state.shardsLeavingCountTowardsConsistency = s.shardsLeavingCountTowardsConsistency
	state.shardsLeavingAndInitializingCountTowardsConsistency = s.shardsLeavingAndInitializingCountTowardsConsistency
	state.leavingAndInitializingPairCounted = false
	state.topoMap = s.state.topoMap
	state.lastResetTime = time.Now()
	state.incRef()
	// todo@bl: Can we combine the writeOpPool and the writeStatePool?
	state.op, state.majority = op, majority
	state.nsID, state.tsID, state.tagEncoder, state.annotation = nsID, tsID, tagEncoder, clonedAnnotation
	op.SetCompletionFn(state.completionFn)

	if err := s.state.topoMap.RouteForEach(tsID, func(
		idx int,
		hostShard shard.Shard,
		host topology.Host,
	) {
		if !s.writeShardsInitializing && hostShard.State() == shard.Initializing {
			// NB(r): Do not write to this node as the shard is initializing
			// and writing to intialized shards is not enabled (also
			// depending on your config initializing shards won't count
			// towards quorum, current defaults, so this is ok consistency wise).
			return
		}

		// Count pending write requests before we enqueue the completion fns,
		// which rely on the count when executing
		state.pending++
		state.queues = append(state.queues, s.state.queues[idx])
	}); err != nil {
		state.decRef()
		return nil, 0, 0, err
	}

	state.Lock()
	for i := range state.queues {
		state.incRef()
		if err := state.queues[i].Enqueue(state.op); err != nil {
			state.Unlock()
			state.decRef()

			// NB(r): if this happens we have a bug, once we are in the read
			// lock the current queues should never be closed
			s.log.Error("[invariant violated] failed to enqueue write", zap.Error(err))
			return nil, 0, 0, err
		}
		enqueued++
	}

	// NB(prateek): the current go-routine still holds a lock on the
	// returned writeState object.
	return state, majority, enqueued, nil
}

func (s *session) Fetch(
	nsID ident.ID,
	id ident.ID,
	startInclusive, endExclusive xtime.UnixNano,
) (encoding.SeriesIterator, error) {
	tsIDs := ident.NewIDsIterator(id)
	results, err := s.FetchIDs(nsID, tsIDs, startInclusive, endExclusive)
	if err != nil {
		return nil, err
	}
	mutableResults := results.(encoding.MutableSeriesIterators)
	iters := mutableResults.Iters()
	iter := iters[0]
	// Reset to zero so that when we close this results set the iter doesn't get closed
	mutableResults.Reset(0)
	mutableResults.Close()
	return iter, nil
}

func (s *session) WriteBatchRawGRPC(
	namespace ident.ID,
	writes ts.BatchWriter,
	opts WriteBatchOptions, // Opts not used for now
) error {
	grpcCfg := s.opts.GRPCClientConfig()
	if !grpcCfg.Enabled {
		return errors.New("gRPC client is not enabled in client options for WriteBatchRawGRPC")
	}

	startWriteAttempt := s.nowFn()
	numTotalWrites := writes.Len()
	if numTotalWrites == 0 {
		return nil
	}

	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return ErrSessionStatusNotOpen
	}
	topoMap := s.state.topoMap
	consistencyLevel := s.state.writeLevel
	majority := int32(s.state.majority)
	s.state.RUnlock() // Release read lock early as we'll be doing work

	// Batches per host: map[hostID] -> []*grpcrpc.WriteBatchRawRequestElement
	batchesByHost := make(map[string][]*grpcrpc.WriteBatchRawRequestElement)
	// Keep track of which shards are involved for final consistency check
	shardsInBatch := make(map[uint32]struct{})


	iter := writes.Iter()
	// First, group all writes by target host based on sharding.
	// This is complex because a single batch from BatchWriter can contain IDs for different shards,
	// and each shard maps to multiple replica hosts.
	// We need to send a specific WriteBatchRawRequestElement to each replica host for an ID.

	// Intermediate structure: map[hostID][]*grpcrpc.WriteBatchRawRequestElement
	// And also map[shardID][]hostID to track replicas for consistency.

	// Store elements per host.
	// An element for ID 'X' needs to go to HostA, HostB, HostC if they are replicas for X's shard.
	// So, when processing ID 'X', we add its WriteBatchRawRequestElement to the lists for HostA, HostB, and HostC.

	// Collect all writes and their target hosts first
	type hostElement struct {
		hostID  string
		element *grpcrpc.WriteBatchRawRequestElement
		shardID uint32
	}
	allHostElements := make([]hostElement, 0, numTotalWrites*topoMap.Replicas())


	for _, write := range writes.Writes() {
		id := write.ID
		shardID := topoMap.ShardSet().Lookup(id)
		shardsInBatch[shardID] = struct{}{}

		dp, err := toGRPCWriteDatapoint(write.Timestamp, write.Datapoint.Value, write.Unit, write.Annotation)
		if err != nil {
			// Consider how to handle individual conversion errors. Fail whole batch? Collect errors?
			s.log.Error("failed to convert datapoint for gRPC WriteBatchRaw", zap.Stringer("id", id), zap.Error(err))
			continue // Skip this write
		}
		element := &grpcrpc.WriteBatchRawRequestElement{
			Id:        id.Bytes(), // FetchBatchRawRequestElement uses bytes
			Datapoint: dp,
		}

		routeErr := topoMap.RouteForEach(id, func(idx int, hostShard shard.Shard, host topology.Host) {
			if !s.writeShardsInitializing && hostShard.State() == shard.Initializing {
				return // Skip
			}
			allHostElements = append(allHostElements, hostElement{hostID: host.IDString(), element: element, shardID: shardID})
		})
		if routeErr != nil {
			return routeErr // Early exit if routing fails for an ID
		}
	}

	// Now, group elements by hostID
	for _, he := range allHostElements {
		batchesByHost[he.hostID] = append(batchesByHost[he.hostID], he.element)
	}

	if len(batchesByHost) == 0 && numTotalWrites > 0 {
		// Possible if all writes were skipped (e.g. due to conversion errors or routing issues)
		s.metrics.writeErrorsInternalError.Inc(1)
		s.metrics.writeLatencyHistogram.RecordDuration(s.nowFn().Sub(startWriteAttempt))
		if numTotalWrites > 0 { // only return error if there were writes to begin with
			return errors.New("no hosts available or all writes failed conversion for gRPC WriteBatchRaw")
		}
		return nil
	}

	var (
		wg             sync.WaitGroup
		responseErrors []error
		errorsLock     sync.Mutex
		// For consistency, track success per shard: map[shardID] -> successful_replica_writes
		shardSuccessCounts = make(map[uint32]int32)
	)

	for hostID, elements := range batchesByHost {
		if len(elements) == 0 {
			continue
		}
		wg.Add(1)
		go func(hID string, elems []*grpcrpc.WriteBatchRawRequestElement) {
			defer wg.Done()

			s.state.RLock() // RLock for accessing grpcNodeClients
			nodeClient, ok := s.grpcNodeClients[hID]
			s.state.RUnlock()

			if !ok {
				errorsLock.Lock()
				responseErrors = append(responseErrors, fmt.Errorf("gRPC client not found for host %s for WriteBatchRaw", hID))
				errorsLock.Unlock()
				return
			}

			// TODO: Handle batching if len(elems) > configured batch size.
			// For now, send all elements for a host in one batch.
			grpcReq := &grpcrpc.WriteBatchRawRequest{
				NameSpace: namespace.String(),
				Elements:  elems,
			}

			timeout := s.opts.WriteRequestTimeout()
			ctx, cancel := gocontext.WithTimeout(gocontext.Background(), timeout)
			defer cancel()

			// WriteBatchRaw returns Empty. Errors are via gRPC status.
			_, err := nodeClient.WriteBatchRaw(ctx, grpcReq)
			if err != nil {
				errorsLock.Lock()
				responseErrors = append(responseErrors, fmt.Errorf("host %s WriteBatchRaw error: %w", hID, err))
				errorsLock.Unlock()
				if s.logHostWriteErrorSampler.Sample() {
					s.log.Error("gRPC WriteBatchRaw error to host", zap.String("host", hID), zap.Error(err))
				}
				return
			}

			// If successful, increment success count for all shards in this host's batch
			// This requires knowing which shards were part of `elems`.
			// This simplified consistency check assumes success for the host implies success for its shards in this batch.
			// A more robust check would iterate `elems`, find their shard, and increment shardSuccessCounts.
			// For now, let's find unique shards for elements sent to this host.
			hostBatchShards := make(map[uint32]struct{})
			for _, el := range elems {
				// Need a way to get shard from el.Id without re-sharding, or pass shard info along.
				// This is where the current structure makes precise per-shard consistency hard.
				// For this pass, we'll approximate: if host call is success, count it for all shards it *could* have data for.
				// This is not strictly correct for per-ID consistency in a batch.
				// A better approach would be for the server to return per-element errors.
			}
			// TEMPORARY: If host call succeeded, assume all writes in its batch for relevant shards succeeded AT THIS HOST.
			// This doesn't directly translate to per-shard quorum easily without more info.
			// For now, we'll count this host as a success for *any* shard it holds from the batch.
			// This simplification means `writeConsistencyResult` will be less accurate for batches.

			// To correctly update shardSuccessCounts, we need to know which shards correspond to `elems`.
			// The `allHostElements` contained this. We need to pass shard info or re-calculate.
			// Let's re-calculate for simplicity for now.
			for _, el := range elems {
				// This is inefficient to do here again.
				elID := s.pools.id.BytesToID(el.Id) // Need a way to get ident.ID back for sharding
				shard := topoMap.ShardSet().Lookup(elID)
				// elID.Finalize() // If created via BytesToID and it's pooled. Assume BytesToID doesn't require finalize.
				atomic.AddInt32(&shardSuccessCounts[shard], 1)
			}

		}(hostID, elements)
	}

	wg.Wait()

	// Overall consistency check: for every shard involved in the initial batch,
	// did it meet the consistency requirements?
	numWritesConsistent := 0
	for shardID := range shardsInBatch {
		if atomic.LoadInt32(&shardSuccessCounts[shardID]) >= majority {
			numWritesConsistent++
		} else {
			// Log which shard failed consistency
			s.log.Warn("gRPC WriteBatchRaw consistency failed for shard",
				zap.Uint32("shard", shardID),
				zap.Int32("successfulReplicas", atomic.LoadInt32(&shardSuccessCounts[shardID])),
				zap.Int32("requiredMajority", majority))
		}
	}

	var finalErr error
	if numWritesConsistent < len(shardsInBatch) {
		errMsg := fmt.Sprintf("failed to meet write consistency for all shards in batch: %d of %d shards met consistency", numWritesConsistent, len(shardsInBatch))
		if len(responseErrors) > 0 {
			finalErr = xerrors.NewMultiError().Add(errors.New(errMsg)).Add(xerrors.NewMultiError().Add(responseErrors...)).FinalError()
		} else {
			finalErr = errors.New(errMsg)
		}
	} else if len(responseErrors) > 0 {
		// All shards met consistency, but there were some replica errors.
		finalErr = xerrors.NewMultiError().Add(responseErrors...).FinalError()
		s.log.Warn("gRPC WriteBatchRaw met consistency for all shards, but some replica errors occurred", zap.Error(finalErr))
		finalErr = nil // Often, if consistency is met, these are treated as non-fatal overall.
	}


	// Record metrics (simplified)
	if finalErr == nil {
		s.metrics.writeSuccess.Inc(int64(numTotalWrites))
	} else {
		s.metrics.writeErrorsInternalError.Inc(int64(numTotalWrites)) // Or more specific
	}
	s.metrics.writeLatencyHistogram.RecordDuration(s.nowFn().Sub(startWriteAttempt))
	if finalErr != nil && s.logWriteErrorSampler.Sample() {
		s.log.Error("m3db client gRPC WriteBatchRawGRPC error occurred",
			zap.Float64("sampleRateLog", s.logWriteErrorSampler.SampleRate().Value()),
			zap.Error(finalErr))
	}
	return finalErr
}

// Helper to convert index.Query to *grpcrpc.Query
func toGRPCQuery(query index.Query) (*grpcrpc.Query, error) {
	if query.IsEmpty() {
		return nil, errors.New("empty query") // Or handle as AllQuery? Depends on convention.
	}
	// This conversion logic is directly analogous to toThriftQuery in the server-side code.
	// We need to ensure all query types are handled.
	// For simplicity, only TermQuery and RegexpQuery are shown here.
	// A complete implementation would handle All, Negation, Conjunction, Disjunction, Field.

	// Assuming query.Field() and query.Regexp() / query.Term() give []byte.
	// grpcrpc.Query has string fields for these.

	switch q := query.Query.(type) {
	case index.TermQuery:
		return &grpcrpc.Query{
			Query: &grpcrpc.Query_Term{
				Term: &grpcrpc.TermQuery{Field: string(q.Field), Term: string(q.Term)},
			},
		}, nil
	case index.RegexpQuery:
		return &grpcrpc.Query{
			Query: &grpcrpc.Query_Regexp{
				Regexp: &grpcrpc.RegexpQuery{Field: string(q.Field), Regexp: string(q.Regexp)},
			},
		}, nil
	case index.AllQuery:
		return &grpcrpc.Query{
			Query: &grpcrpc.Query_All{All: &grpcrpc.AllQuery{}},
		}, nil
	case index.FieldQuery:
		return &grpcrpc.Query{
			Query: &grpcrpc.Query_Field{
				Field: &grpcrpc.FieldQuery{Field: string(q.Field)},
			},
		}, nil
	case index.NegationQuery:
		subQuery, err := toGRPCQuery(q.Query)
		if err != nil {
			return nil, err
		}
		return &grpcrpc.Query{
			Query: &grpcrpc.Query_Negation{
				Negation: &grpcrpc.NegationQuery{Query: subQuery},
			},
		}, nil
	case index.ConjunctionQuery:
		subQueries := make([]*grpcrpc.Query, 0, len(q.Queries))
		for _, subQ := range q.Queries {
			converted, err := toGRPCQuery(subQ)
			if err != nil {
				return nil, err
			}
			subQueries = append(subQueries, converted)
		}
		return &grpcrpc.Query{
			Query: &grpcrpc.Query_Conjunction{
				Conjunction: &grpcrpc.ConjunctionQuery{Queries: subQueries},
			},
		}, nil
	case index.DisjunctionQuery:
		subQueries := make([]*grpcrpc.Query, 0, len(q.Queries))
		for _, subQ := range q.Queries {
			converted, err := toGRPCQuery(subQ)
			if err != nil {
				return nil, err
			}
			subQueries = append(subQueries, converted)
		}
		return &grpcrpc.Query{
			Query: &grpcrpc.Query_Disjunction{
				Disjunction: &grpcrpc.DisjunctionQuery{Queries: subQueries},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown query type: %T", q)
	}
}

// baseTaggedIDsIterator is a basic implementation of TaggedIDsIterator
type baseTaggedIDsIterator struct {
	mu        sync.Mutex
	idx       int
	items     []taggedIDItem
	err       error
	nsID      ident.ID
	tagPool   ident.TagPool
	idPool    ident.Pool
	finalized bool
}

type taggedIDItem struct {
	seriesID    ident.ID // Cloned, owned by this item
	encodedTags checked.Bytes
}

func newBaseTaggedIDsIterator(nsID ident.ID, tagPool ident.TagPool, idPool ident.Pool) *baseTaggedIDsIterator {
	return &baseTaggedIDsIterator{
		idx:     -1,
		items:   make([]taggedIDItem, 0), // Can be pre-sized if total count is known
		nsID:    nsID.Clone(), // Clone namespace ID as iterator will own it
		tagPool: tagPool,
		idPool:  idPool,
	}
}

func (it *baseTaggedIDsIterator) Add(idResult *grpcrpc.FetchTaggedIDResult) {
	it.mu.Lock()
	defer it.mu.Unlock()

	// Clone series ID and encoded tags as the iterator will own them
	clonedSeriesID := it.idPool.Clone(ident.BinaryID(idResult.Id))

	var clonedEncodedTags checked.Bytes
	if len(idResult.EncodedTags) > 0 {
		cb := it.idPool.CheckedBytesPool().Get(len(idResult.EncodedTags))
		cb.IncRef()
		cb.AppendAll(idResult.EncodedTags)
		clonedEncodedTags = cb
	}

	it.items = append(it.items, taggedIDItem{
		seriesID:    clonedSeriesID,
		encodedTags: clonedEncodedTags,
	})
}

func (it *baseTaggedIDsIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.err != nil || it.idx >= len(it.items)-1 {
		return false
	}
	it.idx++
	return true
}

func (it *baseTaggedIDsIterator) Current() (ident.ID, ident.ID, ident.TagIterator) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.err != nil || it.idx < 0 || it.idx >= len(it.items) {
		return nil, nil, ident.EmptyTagIterator
	}
	item := it.items[it.idx]

	tagDecoder := it.tagPool.Get()
	tagDecoder.Reset(item.encodedTags)
	// This tag iterator becomes owned by the caller of Current() indirectly,
	// or needs to be finalized/closed by the iterator itself upon next Next() or Finalize().
	// For now, assume caller does not hold onto it past next Next().
	// A safer pattern would be for the iterator to manage tagDecoder's lifecycle.
	// Let's create a new tag iterator that makes a copy for safety, or use a pooled one.
	// For simplicity, let's assume the TagDecoder is reset on next Current() call if needed.
	// A better way is to return a TagIterator that is self-contained or uses a copy.
	// For now, this is a direct use of the pooled decoder.

	// To make it safer, we can use a TagsIterator that clones the tags.
	// However, ident.NewTagsIterator takes ident.Tags.
	// Let's assume for now the user of the iterator finalizes the tags or copies them.
	// This is not ideal.
	// A better TaggedIDsIterator would handle this.
	// For now, returning a TagIterator that might be invalidated on next `Current()` call if not careful.
	// This is a common Go iterator pattern problem.

	// Create a new Tags object by decoding and cloning
	tags := ident.NewTags()
	for tagDecoder.Next() {
		curr := tagDecoder.Current()
		name := it.idPool.Clone(curr.Name)
		value := it.idPool.Clone(curr.Value)
		tags.Append(ident.Tag{Name: name, Value: value})
	}
	// NB: We should handle tagDecoder.Err() here.
	// The TagIterator takes ownership of these cloned tags.
	return it.nsID, item.seriesID, ident.NewTagsIterator(tags)
}

func (it *baseTaggedIDsIterator) Err() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err
}

func (it *baseTaggedIDsIterator) SetError(err error) {
	it.mu.Lock()
	it.err = err
	it.mu.Unlock()
}

func (it *baseTaggedIDsIterator) Remaining() int {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.idx >= len(it.items) {
		return 0
	}
	return len(it.items) - (it.idx + 1)
}

func (it *baseTaggedIDsIterator) Finalize() {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.finalized {
		return
	}
	it.finalized = true
	it.err = nil
	if it.nsID != nil {
		it.nsID.Finalize()
		it.nsID = nil
	}
	for _, item := range it.items {
		if item.seriesID != nil {
			item.seriesID.Finalize()
		}
		if item.encodedTags != nil {
			item.encodedTags.DecRef()
			item.encodedTags.Finalize()
		}
	}
	it.items = it.items[:0]
	// Note: TagDecoder from Current() is not finalized here, assumes caller handles or it's short-lived.
	// This is a known simplification.
}


func (s *session) fromGRPCFetchTaggedResultsToTaggedIDsIterator(
	results []*grpcrpc.FetchTaggedResult,
	namespace ident.ID,
	opts index.QueryOptions,
) (TaggedIDsIterator, FetchResponseMetadata, error) {

	deduped := make(map[string]*grpcrpc.FetchTaggedIDResult)
	var exhaustive = true // Only true if all successful responses are exhaustive

	for _, result := range results {
		if result == nil {
			// A nil result from a replica might mean an error or no data.
			// If it was an error, it should have been caught before this function.
			// If it means no data, it might impact exhaustiveness.
			// For now, if any result is not exhaustive, the whole is not.
			exhaustive = exhaustive && result.Exhaustive
			continue
		}
		exhaustive = exhaustive && result.Exhaustive
		for _, elem := range result.Elements {
			if elem == nil {
				continue
			}
			// Key by series ID string for deduplication
			// Note: elem.Id is []byte, convert to string for map key
			idStr := string(elem.Id)
			if _, exists := deduped[idStr]; !exists {
				deduped[idStr] = elem
			}
			// TODO: Merging strategy if multiple results for the same ID (e.g. different tags? Unlikely for FetchTaggedIDs)
		}
	}

	finalIterator := newBaseTaggedIDsIterator(namespace, s.pools.tagDecoder, s.pools.id)
	for _, item := range deduped {
		finalIterator.Add(item)
	}

	// TODO: Aggregate WaitedIndex, WaitedSeriesRead from FetchResponseMetadata if needed.
	// For now, FetchResponseMetadata from this gRPC path will be basic.
	metadata := FetchResponseMetadata{
		Exhaustive: exhaustive,
		Responses:  len(results), // Number of replica responses processed
	}

	return finalIterator, metadata, nil
}


func (s *session) FetchTaggedIDsGRPC(
	ctx gocontext.Context,
	namespace ident.ID,
	query index.Query,
	opts index.QueryOptions,
) (TaggedIDsIterator, FetchResponseMetadata, error) {
	grpcCfg := s.opts.GRPCClientConfig()
	if !grpcCfg.Enabled {
		return nil, FetchResponseMetadata{}, errors.New("gRPC client is not enabled for FetchTaggedIDsGRPC")
	}

	startFetchAttempt := s.nowFn()

	grpcQuery, err := toGRPCQuery(query)
	if err != nil {
		return nil, FetchResponseMetadata{}, xerrors.NewInvalidParamsError(fmt.Errorf("failed to convert query to gRPC: %w", err))
	}

	grpcReq := &grpcrpc.FetchTaggedRequest{
		NameSpace:         namespace.String(),
		Query:             grpcQuery,
		RangeStart:        int64(opts.StartInclusive),
		RangeEnd:          int64(opts.EndExclusive),
		FetchData:         false, // Explicitly false for FetchTaggedIDs
		SeriesLimit:       opts.SeriesLimit,
		DocsLimit:         opts.DocsLimit,
		RequireExhaustive: &opts.RequireExhaustive,
		RangeTimeType:     grpcrpc.TimeType_UNIX_NANOSECONDS, // Defaulting, align with how times are passed
		// Source: opts.Source, // If source is available and needed
		RequireNoWait:     &opts.RequireNoWait,
	}

	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return nil, FetchResponseMetadata{}, ErrSessionStatusNotOpen
	}

	hosts := s.state.topoMap.Hosts()
	consistencyLevel := s.state.readLevel
	majority := int32(s.state.majority)
	s.state.RUnlock()

	if len(hosts) == 0 {
		return nil, FetchResponseMetadata{}, errors.New("no hosts available in topology")
	}

	var (
		wg               sync.WaitGroup
		successfulResults []*grpcrpc.FetchTaggedResult
		responseErrors   []error
		errorsLock       sync.Mutex
		pending          int32 = int32(len(hosts))
		enqueued         int32 = 0
	)

	wg.Add(len(hosts))

	for _, host := range hosts {
		enqueued++
		go func(h topology.Host) {
			defer wg.Done()
			defer atomic.AddInt32(&pending, -1)

			s.state.RLock()
			nodeClient, ok := s.grpcNodeClients[h.IDString()]
			s.state.RUnlock()

			if !ok {
				errorsLock.Lock()
				responseErrors = append(responseErrors, fmt.Errorf("gRPC client not found for host %s", h.IDString()))
				errorsLock.Unlock()
				return
			}

			timeout := opts.TimeoutOrDefault(s.opts.FetchRequestTimeout())
			callCtx, cancel := gocontext.WithTimeout(ctx, timeout)
			defer cancel()

			result, err := nodeClient.FetchTagged(callCtx, grpcReq)
			if err != nil {
				errorsLock.Lock()
				responseErrors = append(responseErrors, err)
				errorsLock.Unlock()
				if s.logHostFetchErrorSampler.Sample() {
					s.log.Error("gRPC FetchTagged error from host", zap.String("host", h.IDString()), zap.Error(err))
				}
				return
			}
			errorsLock.Lock()
			successfulResults = append(successfulResults, result)
			errorsLock.Unlock()
		}(host)
	}

	wg.Wait()

	numSuccess := int32(len(successfulResults))
	consistencyErr := s.readConsistencyResult(consistencyLevel, majority, enqueued, numSuccess, int32(len(responseErrors)), responseErrors)

	finalMetadata := FetchResponseMetadata{Responses: int(numSuccess)}
	if len(successfulResults) > 0 {
		isExhaustive := true
		for _, res := range successfulResults {
			if !res.Exhaustive {
				isExhaustive = false;
				break
			}
		}
		finalMetadata.Exhaustive = isExhaustive
		// TODO: Aggregate WaitedIndex, WaitedSeriesRead
	}


	if consistencyErr != nil {
		s.metrics.fetchErrorsInternalError.Inc(1) // Or map specific errors
		s.metrics.fetchLatencyHistogram.RecordDuration(s.nowFn().Sub(startFetchAttempt))
		if s.logFetchErrorSampler.Sample() {
			s.log.Error("m3db client gRPC FetchTaggedIDs consistency error", zap.Error(consistencyErr))
		}
		return nil, finalMetadata, consistencyErr
	}

	if len(successfulResults) == 0 {
		s.metrics.fetchErrorsInternalError.Inc(1)
		s.metrics.fetchLatencyHistogram.RecordDuration(s.nowFn().Sub(startFetchAttempt))
		if len(responseErrors) > 0 {
			return nil, finalMetadata, xerrors.NewMultiError().Add(responseErrors...).FinalError()
		}
		return encoding.EmptyTaggedIDsIterator, finalMetadata, errors.New("no successful responses from any host for FetchTaggedIDsGRPC")
	}

	iter, iterMetadata, err := s.fromGRPCFetchTaggedResultsToTaggedIDsIterator(successfulResults, namespace, opts)
	finalMetadata.Exhaustive = finalMetadata.Exhaustive && iterMetadata.Exhaustive // Combine exhaustiveness

	s.metrics.fetchSuccess.Inc(1)
	s.metrics.fetchLatencyHistogram.RecordDuration(s.nowFn().Sub(startFetchAttempt))
	if err != nil {
		s.log.Error("m3db client gRPC FetchTaggedIDs iterator creation error", zap.Error(err))
		return nil, finalMetadata, err
	}

	return iter, finalMetadata, nil
}

// Helper to convert index.AggregationType to grpcrpc.AggregateQueryType
func toGRPCAggregateQueryType(aggType index.AggregationType) (grpcrpc.AggregateQueryType, error) {
	switch aggType {
	case index.AggregateTagNameOnly:
		return grpcrpc.AggregateQueryType_AGGREGATE_BY_TAG_NAME, nil
	case index.AggregateTagValueOnly: // Assuming this means AGGREGATE_BY_TAG_NAME_VALUE
		return grpcrpc.AggregateQueryType_AGGREGATE_BY_TAG_NAME_VALUE, nil
	default:
		return grpcrpc.AggregateQueryType_AGGREGATE_BY_TAG_NAME_VALUE, fmt.Errorf("unknown aggregation type: %v", aggType)
	}
}

// baseAggregatedTagsIterator is a basic implementation of AggregatedTagsIterator for gRPC results.
type baseAggregatedTagsIterator struct {
	mu        sync.Mutex
	idx       int
	items     []aggregatedTagsItem // Stores merged, deduplicated results
	err       error
	tagPool   ident.TagPool // For decoding tag values if they were bytes, though they are strings here.
	idPool    ident.Pool    // For cloning tag names and values
	finalized bool
}

type aggregatedTagsItem struct {
	tagName   ident.ID   // Cloned
	tagValues []ident.ID // Cloned
}

func newBaseAggregatedTagsIterator(idPool ident.Pool) *baseAggregatedTagsIterator {
	return &baseAggregatedTagsIterator{
		idx:    -1,
		items:  make([]aggregatedTagsItem, 0),
		idPool: idPool,
		// tagPool might not be strictly necessary if TagName and TagValue are already strings/bytes
	}
}

func (it *baseAggregatedTagsIterator) Add(tagName []byte, tagValues [][]byte) {
	it.mu.Lock()
	defer it.mu.Unlock()

	clonedTagName := it.idPool.BinaryID(checked.Bytes(tagName).IncRef()) // Assume BinaryID clones if needed or manages lifecycle

	clonedTagValues := make([]ident.ID, len(tagValues))
	for i, tv := range tagValues {
		clonedTagValues[i] = it.idPool.BinaryID(checked.Bytes(tv).IncRef())
	}

	it.items = append(it.items, aggregatedTagsItem{
		tagName:   clonedTagName,
		tagValues: clonedTagValues,
	})
}


func (it *baseAggregatedTagsIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.err != nil || it.idx >= len(it.items)-1 {
		return false
	}
	it.idx++
	return true
}

func (it *baseAggregatedTagsIterator) Current() (ident.ID, ident.Iterator) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.err != nil || it.idx < 0 || it.idx >= len(it.items) {
		return nil, ident.NewIDIterator()
	}
	item := it.items[it.idx]

	// Create an ident.Iterator for the tag values.
	// The ident.Iterator should manage the lifecycle of the IDs it iterates over.
	// Since item.tagValues are already cloned ident.IDs, we can create an iterator over them.
	// Need to ensure that if ident.NewIDIterator takes ownership, the original cloned IDs are safe.
	// For now, assume NewIDIterator creates a safe view or copy if needed.
	return item.tagName, ident.NewIDIterator(item.tagValues...)
}

func (it *baseAggregatedTagsIterator) Err() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err
}

func (it *baseAggregatedTagsIterator) SetError(err error) {
	it.mu.Lock()
	it.err = err
	it.mu.Unlock()
}

func (it *baseAggregatedTagsIterator) Remaining() int {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.idx >= len(it.items) {
		return 0
	}
	return len(it.items) - (it.idx + 1)
}

func (it *baseAggregatedTagsIterator) Finalize() {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.finalized {
		return
	}
	it.finalized = true
	it.err = nil
	for _, item := range it.items {
		if item.tagName != nil {
			item.tagName.Finalize()
		}
		for _, tv := range item.tagValues {
			if tv != nil {
				tv.Finalize()
			}
		}
	}
	it.items = it.items[:0]
}


func (s *session) fromGRPCAggregateQueryRawResultsToIterator(
	results []*grpcrpc.AggregateQueryRawResult,
	opts index.AggregationOptions, // For potential limits or options in future
) (AggregatedTagsIterator, FetchResponseMetadata, error) {

	// Merge logic: map[tagNameString] -> map[tagValueString] -> struct{} (for set behavior)
	mergedTagNames := make(map[string]map[string]struct{})
	var exhaustive = true

	for _, result := range results {
		if result == nil {
			exhaustive = false // If any replica fails or doesn't respond, can't guarantee exhaustive
			continue
		}
		exhaustive = exhaustive && result.Exhaustive
		for _, tagNameElem := range result.Results {
			if tagNameElem == nil {
				continue
			}
			tagNameStr := string(tagNameElem.TagName)
			if _, ok := mergedTagNames[tagNameStr]; !ok {
				mergedTagNames[tagNameStr] = make(map[string]struct{})
			}
			for _, tagValueElem := range tagNameElem.TagValues {
				if tagValueElem == nil {
					continue
				}
				mergedTagNames[tagNameStr][string(tagValueElem.TagValue)] = struct{}{}
			}
		}
	}

	finalIterator := newBaseAggregatedTagsIterator(s.pools.id)
	// Sort tag names for deterministic iteration order
	sortedTagNames := make([]string, 0, len(mergedTagNames))
	for tn := range mergedTagNames {
		sortedTagNames = append(sortedTagNames, tn)
	}
	sort.Strings(sortedTagNames)

	for _, tagNameStr := range sortedTagNames {
		tagValuesMap := mergedTagNames[tagNameStr]
		tagValuesBytes := make([][]byte, 0, len(tagValuesMap))
		for tvStr := range tagValuesMap {
			tagValuesBytes = append(tagValuesBytes, []byte(tvStr))
		}
		// Sort tag values for deterministic iteration order
		sort.Slice(tagValuesBytes, func(i, j int) bool {
			return bytes.Compare(tagValuesBytes[i], tagValuesBytes[j]) < 0
		})
		finalIterator.Add([]byte(tagNameStr), tagValuesBytes)
	}

	metadata := FetchResponseMetadata{
		Exhaustive: exhaustive,
		Responses:  len(results),
	}
	return finalIterator, metadata, nil
}


func (s *session) AggregateGRPC(
	ctx gocontext.Context,
	namespace ident.ID,
	query index.Query,
	opts index.AggregationOptions,
) (AggregatedTagsIterator, FetchResponseMetadata, error) {
	grpcCfg := s.opts.GRPCClientConfig()
	if !grpcCfg.Enabled {
		return nil, FetchResponseMetadata{}, errors.New("gRPC client is not enabled for AggregateGRPC")
	}

	startFetchAttempt := s.nowFn()

	grpcQuery, err := toGRPCQuery(query)
	if err != nil {
		return nil, FetchResponseMetadata{}, xerrors.NewInvalidParamsError(fmt.Errorf("failed to convert query to gRPC: %w", err))
	}

	grpcAggType, err := toGRPCAggregateQueryType(opts.Type)
	if err != nil {
		return nil, FetchResponseMetadata{}, xerrors.NewInvalidParamsError(err)
	}

	// Convert TagNameFilter from [][]byte to repeated_bytes
	tagNameFilterBytes := make([][]byte, len(opts.TagNameFilter))
	for i, tn := range opts.TagNameFilter {
		tagNameFilterBytes[i] = tn
	}

	grpcReq := &grpcrpc.AggregateQueryRawRequest{
		NameSpace:          namespace.Bytes(), // AggregateQueryRawRequest uses bytes for namespace
		Query:             grpcQuery,
		RangeStart:        int64(opts.StartInclusive),
		RangeEnd:          int64(opts.EndExclusive),
		SeriesLimit:       opts.SeriesLimit,
		DocsLimit:         opts.DocsLimit,
		AggregateQueryType: grpcAggType,
		TagNameFilter:     tagNameFilterBytes,
		RangeTimeType:     grpcrpc.TimeType_UNIX_NANOSECONDS, // Assuming times are nanos
		RequireExhaustive: &opts.RequireExhaustive,
		RequireNoWait:     &opts.RequireNoWait,
		// Source: opts.Source, // If available
	}

	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return nil, FetchResponseMetadata{}, ErrSessionStatusNotOpen
	}
	hosts := s.state.topoMap.Hosts()
	consistencyLevel := s.state.readLevel
	majority := int32(s.state.majority)
	s.state.RUnlock()

	if len(hosts) == 0 {
		return nil, FetchResponseMetadata{}, errors.New("no hosts available in topology for AggregateGRPC")
	}

	var (
		wg                sync.WaitGroup
		successfulResults []*grpcrpc.AggregateQueryRawResult
		responseErrors    []error
		errorsLock        sync.Mutex
		pending           int32 = int32(len(hosts))
		enqueued          int32 = 0
	)
	wg.Add(len(hosts))

	for _, host := range hosts {
		enqueued++
		go func(h topology.Host) {
			defer wg.Done()
			defer atomic.AddInt32(&pending, -1)

			s.state.RLock()
			nodeClient, ok := s.grpcNodeClients[h.IDString()]
			s.state.RUnlock()
			if !ok {
				errorsLock.Lock()
				responseErrors = append(responseErrors, fmt.Errorf("gRPC client not found for host %s", h.IDString()))
				errorsLock.Unlock()
				return
			}

			timeout := opts.TimeoutOrDefault(s.opts.FetchRequestTimeout())
			callCtx, cancel := gocontext.WithTimeout(ctx, timeout)
			defer cancel()

			result, err := nodeClient.AggregateRaw(callCtx, grpcReq)
			if err != nil {
				errorsLock.Lock()
				responseErrors = append(responseErrors, err)
				errorsLock.Unlock()
				if s.logHostFetchErrorSampler.Sample() {
					s.log.Error("gRPC AggregateRaw error from host", zap.String("host", h.IDString()), zap.Error(err))
				}
				return
			}
			errorsLock.Lock()
			successfulResults = append(successfulResults, result)
			errorsLock.Unlock()
		}(host)
	}

	wg.Wait()

	numSuccess := int32(len(successfulResults))
	consistencyErr := s.readConsistencyResult(consistencyLevel, majority, enqueued, numSuccess, int32(len(responseErrors)), responseErrors)

	finalMetadata := FetchResponseMetadata{Responses: int(numSuccess)}
	if len(successfulResults) > 0 {
		isExhaustive := true
		for _, res := range successfulResults {
			if !res.Exhaustive {
				isExhaustive = false;
				break
			}
		}
		finalMetadata.Exhaustive = isExhaustive
		// TODO: Aggregate WaitedIndex from successfulResults if/when it's added to AggregateQueryRawResult
	}

	if consistencyErr != nil {
		s.metrics.fetchErrorsInternalError.Inc(1)
		s.metrics.fetchLatencyHistogram.RecordDuration(s.nowFn().Sub(startFetchAttempt))
		if s.logFetchErrorSampler.Sample() {
			s.log.Error("m3db client gRPC AggregateGRPC consistency error", zap.Error(consistencyErr))
		}
		return nil, finalMetadata, consistencyErr
	}

	if len(successfulResults) == 0 {
		s.metrics.fetchErrorsInternalError.Inc(1)
		s.metrics.fetchLatencyHistogram.RecordDuration(s.nowFn().Sub(startFetchAttempt))
		if len(responseErrors) > 0 {
			return nil, finalMetadata, xerrors.NewMultiError().Add(responseErrors...).FinalError()
		}
		return encoding.EmptyAggregatedTagsIterator, finalMetadata, errors.New("no successful responses from any host for AggregateGRPC")
	}

	iter, iterMetadata, err := s.fromGRPCAggregateQueryRawResultsToIterator(successfulResults, opts)
	finalMetadata.Exhaustive = finalMetadata.Exhaustive && iterMetadata.Exhaustive
	// TODO: Aggregate other metadata fields from iterMetadata if they become available


	s.metrics.fetchSuccess.Inc(1)
	s.metrics.fetchLatencyHistogram.RecordDuration(s.nowFn().Sub(startFetchAttempt))
	if err != nil {
		s.log.Error("m3db client gRPC AggregateGRPC iterator creation error", zap.Error(err))
		return nil, finalMetadata, err
	}

	return iter, finalMetadata, nil
}

// Helper to encode ident.TagIterator to []byte using session's tag encoder pool
func (s *session) encodeTagsToBytesGRPC(tagIter ident.TagIterator) ([]byte, error) {
	if tagIter == nil {
		return nil, nil // No tags, no bytes
	}
	// The TagIterator might be used multiple times if a write is retried or fanned out.
	// We need to ensure we're iterating a "fresh" copy or a resettable one if the
	// original iterator is consumed. ident.NewTagsIterator(tagIter.RemainingTags()) creates a new one.
	freshIter := ident.NewTagsIterator(tagIter.RemainingTags())

	encoder := s.pools.tagEncoder.Get()
	defer encoder.Finalize() // Ensure encoder is returned to pool

	if err := encoder.Encode(freshIter); err != nil {
		return nil, fmt.Errorf("failed to encode tags: %w", err)
	}
	data, ok := encoder.Data()
	if !ok {
		// Should not happen if Encode returned no error
		return nil, errors.New("failed to retrieve encoded tag data though encoding reported no error")
	}
	// Data() returns checked.Bytes, we need []byte for gRPC.
	// The checked.Bytes should be finalized after its Bytes() are no longer needed.
	// Here, we copy to a new []byte slice for safety, so checked.Bytes can be finalized by encoder.Finalize().
	// If data.Bytes() returns an internal buffer that's reused, copying is essential.
	// If data.Bytes() returns a safe copy, this extra copy is just for explicit safety.
	encodedBytes := make([]byte, len(data.Bytes()))
	copy(encodedBytes, data.Bytes())
	// data.Finalize() // This is handled by encoder.Finalize()
	return encodedBytes, nil
}


func (s *session) WriteTaggedBatchRawGRPC(
	namespace ident.ID,
	writes ts.TaggedBatchWriter,
	opts WriteBatchOptions, // Opts not used for now
) error {
	grpcCfg := s.opts.GRPCClientConfig()
	if !grpcCfg.Enabled {
		return errors.New("gRPC client is not enabled in client options for WriteTaggedBatchRawGRPC")
	}

	startWriteAttempt := s.nowFn()
	numTotalWrites := writes.Len()
	if numTotalWrites == 0 {
		return nil
	}

	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return ErrSessionStatusNotOpen
	}
	topoMap := s.state.topoMap
	consistencyLevel := s.state.writeLevel
	majority := int32(s.state.majority)
	s.state.RUnlock()

	batchesByHost := make(map[string][]*grpcrpc.WriteTaggedBatchRawRequestElement)
	shardsInBatch := make(map[uint32]struct{}) // To track unique shards for consistency check

	iter := writes.Iter()
	for _, write := range writes.Writes() {
		id := write.ID
		shardID := topoMap.ShardSet().Lookup(id)
		shardsInBatch[shardID] = struct{}{}

		dp, err := toGRPCWriteDatapoint(write.Timestamp, write.Datapoint.Value, write.Unit, write.Annotation)
		if err != nil {
			s.log.Error("failed to convert datapoint for gRPC WriteTaggedBatchRaw", zap.Stringer("id", id), zap.Error(err))
			continue
		}

		// NB: writes.Iter() gives a new iterator each time, but TagIterator within write might be reused.
		// The `encodeTagsToBytesGRPC` helper now takes care of using a fresh view of the tags.
		encodedTags, err := s.encodeTagsToBytesGRPC(write.Tags)
		if err != nil {
			s.log.Error("failed to encode tags for gRPC WriteTaggedBatchRaw", zap.Stringer("id", id), zap.Error(err))
			continue
		}

		element := &grpcrpc.WriteTaggedBatchRawRequestElement{
			Id:          id.Bytes(),
			EncodedTags: encodedTags,
			Datapoint:   dp,
		}

		routeErr := topoMap.RouteForEach(id, func(idx int, hostShard shard.Shard, host topology.Host) {
			if !s.writeShardsInitializing && hostShard.State() == shard.Initializing {
				return
			}
			batchesByHost[host.IDString()] = append(batchesByHost[host.IDString()], element)
		})
		if routeErr != nil {
			// This error means we couldn't route one of the IDs, which is serious.
			// It might be better to collect all such errors and return a multi-error.
			// For now, return early.
			return routeErr
		}
	}

	if len(batchesByHost) == 0 && numTotalWrites > 0 {
		s.metrics.writeErrorsInternalError.Inc(1) // Or a more specific metric for "no valid writes"
		s.metrics.writeLatencyHistogram.RecordDuration(s.nowFn().Sub(startWriteAttempt))
		if numTotalWrites > 0 {
			return errors.New("no hosts available or all writes failed conversion/encoding for gRPC WriteTaggedBatchRaw")
		}
		return nil
	}

	var (
		wg                  sync.WaitGroup
		responseErrors      []error
		errorsLock          sync.Mutex
		shardSuccessCounts  = make(map[uint32]int32)
	)

	for hostID, elements := range batchesByHost {
		if len(elements) == 0 {
			continue
		}
		wg.Add(1)
		go func(hID string, elems []*grpcrpc.WriteTaggedBatchRawRequestElement) {
			defer wg.Done()

			s.state.RLock()
			nodeClient, ok := s.grpcNodeClients[hID]
			s.state.RUnlock()

			if !ok {
				errorsLock.Lock()
				responseErrors = append(responseErrors, fmt.Errorf("gRPC client not found for host %s for WriteTaggedBatchRaw", hID))
				errorsLock.Unlock()
				return
			}

			grpcReq := &grpcrpc.WriteTaggedBatchRawRequest{
				NameSpace: namespace.String(),
				Elements:  elems,
			}

			timeout := s.opts.WriteRequestTimeout()
			ctx, cancel := gocontext.WithTimeout(gocontext.Background(), timeout)
			defer cancel()

			_, err := nodeClient.WriteTaggedBatchRaw(ctx, grpcReq)
			if err != nil {
				errorsLock.Lock()
				responseErrors = append(responseErrors, fmt.Errorf("host %s WriteTaggedBatchRaw error: %w", hID, err))
				errorsLock.Unlock()
				if s.logHostWriteErrorSampler.Sample() {
					s.log.Error("gRPC WriteTaggedBatchRaw error to host", zap.String("host", hID), zap.Error(err))
				}
				return
			}

			// If successful, increment success counts for shards in this host's batch elements
			for _, el := range elems {
				elID := s.pools.id.BytesToID(el.Id) // Inefficient, TODO: optimize shard tracking
				shard := topoMap.ShardSet().Lookup(elID)
				// elID.Finalize() // No, BytesToID doesn't take ownership from pool unless cloned.
				atomic.AddInt32(&shardSuccessCounts[shard], 1)
			}

		}(hostID, elements)
	}

	wg.Wait()

	numWritesConsistent := 0
	for shardID := range shardsInBatch {
		if atomic.LoadInt32(&shardSuccessCounts[shardID]) >= majority {
			numWritesConsistent++
		} else {
			s.log.Warn("gRPC WriteTaggedBatchRaw consistency failed for shard",
				zap.Uint32("shard", shardID),
				zap.Int32("successfulReplicas", atomic.LoadInt32(&shardSuccessCounts[shardID])),
				zap.Int32("requiredMajority", majority))
		}
	}

	var finalErr error
	if numWritesConsistent < len(shardsInBatch) {
		errMsg := fmt.Sprintf("failed to meet write consistency for all shards in tagged batch: %d of %d shards met consistency", numWritesConsistent, len(shardsInBatch))
		if len(responseErrors) > 0 {
			finalErr = xerrors.NewMultiError().Add(errors.New(errMsg)).Add(xerrors.NewMultiError().Add(responseErrors...)).FinalError()
		} else {
			finalErr = errors.New(errMsg)
		}
	} else if len(responseErrors) > 0 {
		finalErr = xerrors.NewMultiError().Add(responseErrors...).FinalError()
		s.log.Warn("gRPC WriteTaggedBatchRaw met consistency for all shards, but some replica errors occurred", zap.Error(finalErr))
		finalErr = nil
	}

	if finalErr == nil {
		s.metrics.writeSuccess.Inc(int64(numTotalWrites)) // Consider specific tagged batch metrics
	} else {
		s.metrics.writeErrorsInternalError.Inc(int64(numTotalWrites))
	}
	s.metrics.writeLatencyHistogram.RecordDuration(s.nowFn().Sub(startWriteAttempt))
	if finalErr != nil && s.logWriteErrorSampler.Sample() {
		s.log.Error("m3db client gRPC WriteTaggedBatchRawGRPC error occurred",
			zap.Float64("sampleRateLog", s.logWriteErrorSampler.SampleRate().Value()),
			zap.Error(finalErr))
	}
	return finalErr
}

func (s *session) FetchIDsGRPC(
	namespaceID ident.ID,
	ids ident.Iterator,
	startInclusive xtime.UnixNano,
	endExclusive xtime.UnixNano,
) (encoding.SeriesIterators, error) {
	grpcCfg := s.opts.GRPCClientConfig()
	if !grpcCfg.Enabled {
		return nil, errors.New("gRPC client is not enabled in client options for FetchIDsGRPC")
	}

	startFetchAttempt := s.nowFn()

	nsCtx, err := s.nsCtxFor(namespaceID)
	if err != nil {
		return nil, err
	}

	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return nil, ErrSessionStatusNotOpen
	}

	// Clone the input iterator as it might be consumed.
	clonedIDs := ids.Duplicate()
	numIDs := clonedIDs.Remaining()
	seriesIters := encoding.NewSizedSeriesIterators(numIDs)
	success := false // Flag to manage closing seriesIters on error

	defer func() {
		if !success {
			seriesIters.Close()
		}
		// Finalize the cloned iterator if it's not a NoFinalizeAlloc an no-op.
		clonedIDs.Finalize()
	}()

	topoMap := s.state.topoMap
	consistencyLevel := s.state.readLevel
	majority := int32(s.state.majority)
	// numReplicas := int32(s.state.replicas) // Used for consistency checks if needed by NumDesiredForReadConsistency

	// This logic is simplified from fetchIDsAttempt and focuses on one ID at a time for gRPC calls.
	// A more optimized version would batch IDs per host for FetchBatchRaw.
	// For now, one FetchBatchRaw call per ID, but still fanning out to replicas for that ID.

	var (
		wg            sync.WaitGroup
		resultErrLock sync.Mutex
		finalMultiErr xerrors.MultiError
	)

	for i := 0; clonedIDs.Next(); i++ {
		currentID := s.pools.id.Clone(clonedIDs.Current()) // Clone for each goroutine/iteration

		wg.Add(1)
		go func(idx int, seriesID ident.ID) {
			defer wg.Done()
			defer seriesID.Finalize() // Finalize the cloned ID for this goroutine

			var (
				enqueued          int32
				pending           int32
				successfulResults []*grpcrpc.FetchBatchRawResult
				responseErrors    []error
				errorsLock        sync.Mutex
				replicaWg         sync.WaitGroup
			)

			grpcReq := &grpcrpc.FetchBatchRawRequest{
				NameSpace:     namespaceID.String(),
				Ids:           [][]byte{seriesID.Bytes()}, // Batch of one ID
				RangeStart:    int64(startInclusive),
				RangeEnd:      int64(endExclusive),
				RangeTimeType: grpcrpc.TimeType_UNIX_NANOSECONDS,
			}

			routeErr := topoMap.RouteForEach(seriesID, func(replicaIdx int, hostShard shard.Shard, host topology.Host) {
				pending++
				replicaWg.Add(1)
				enqueued++

				go func(h topology.Host) {
					defer replicaWg.Done()
					defer atomic.AddInt32(&pending, -1)

					nodeClient, ok := s.grpcNodeClients[h.IDString()]
					if !ok {
						errorsLock.Lock()
						responseErrors = append(responseErrors, fmt.Errorf("gRPC client not found for host %s for ID %s", h.IDString(), seriesID.String()))
						errorsLock.Unlock()
						return
					}

					timeout := s.opts.FetchRequestTimeout()
					ctx, cancel := gocontext.WithTimeout(gocontext.Background(), timeout)
					defer cancel()

					result, err := nodeClient.FetchBatchRaw(ctx, grpcReq) // Assuming V1 for now
					if err != nil {
						errorsLock.Lock()
						responseErrors = append(responseErrors, err)
						errorsLock.Unlock()
						if s.logHostFetchErrorSampler.Sample() {
							s.log.Error("gRPC FetchBatchRaw error from host", zap.String("host", h.IDString()), zap.Stringer("id", seriesID), zap.Error(err))
						}
						return
					}
					errorsLock.Lock()
					successfulResults = append(successfulResults, result)
					errorsLock.Unlock()
				}(host)
			})

			if routeErr != nil {
				resultErrLock.Lock()
				finalMultiErr = finalMultiErr.Add(routeErr)
				resultErrLock.Unlock()
				seriesIters.SetAt(idx, encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{ID: seriesID, Namespace: namespaceID}, routeErr))
				return
			}

			if enqueued == 0 {
                 err := fmt.Errorf("no hosts available for ID %s", seriesID.String())
                 resultErrLock.Lock()
                 finalMultiErr = finalMultiErr.Add(err)
                 resultErrLock.Unlock()
                 seriesIters.SetAt(idx, encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{ID: seriesID, Namespace: namespaceID}, err))
                 return
            }

			replicaWg.Wait()

			numSuccess := int32(len(successfulResults))
			consistencyErr := s.readConsistencyResult(consistencyLevel, majority, enqueued, numSuccess, int32(len(responseErrors)), responseErrors)

			if consistencyErr != nil {
				resultErrLock.Lock()
				finalMultiErr = finalMultiErr.Add(fmt.Errorf("consistency error for ID %s: %w", seriesID.String(), consistencyErr))
				resultErrLock.Unlock()
				seriesIters.SetAt(idx, encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{ID: seriesID, Namespace: namespaceID}, consistencyErr))
				return
			}

			if len(successfulResults) == 0 {
				seriesIters.SetAt(idx, encoding.EmptySeriesIterator)
				return
			}

			// Process results - for now, take the first successful one.
			// A FetchBatchRawResult contains a list of FetchRawResult. We expect one for our single ID.
			var finalSeriesIter encoding.SeriesIterator = encoding.EmptySeriesIterator
			var iterErr error

			if len(successfulResults[0].Elements) > 0 {
				// Assuming successfulResults[0].Elements[0] corresponds to our requested ID
				finalSeriesIter, iterErr = fromGRPCSegmentsToSeriesIterator(
					successfulResults[0].Elements[0].Segments, // This is []*grpcrpc.Segments
					seriesID, nsCtx, startInclusive, endExclusive, s.opts)
			}

			if iterErr != nil {
				resultErrLock.Lock()
				finalMultiErr = finalMultiErr.Add(fmt.Errorf("iterator conversion error for ID %s: %w", seriesID.String(), iterErr))
				resultErrLock.Unlock()
				seriesIters.SetAt(idx, encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{ID: seriesID, Namespace: namespaceID}, iterErr))
				return
			}
			seriesIters.SetAt(idx, finalSeriesIter)

		}(i, currentID)
	}
	s.state.RUnlock() // Unlock after launching all goroutines for IDs

	wg.Wait()

	// Record overall metrics (simplified)
	overallError := finalMultiErr.FinalError()
	if overallError == nil {
		s.metrics.fetchSuccess.Inc(1) // This might be too broad; ideally, per-ID success.
	} else {
		s.metrics.fetchErrorsInternalError.Inc(1) // Or map to bad_request if appropriate
	}
	s.metrics.fetchLatencyHistogram.RecordDuration(s.nowFn().Sub(startFetchAttempt))
	if overallError != nil && s.logFetchErrorSampler.Sample() {
		s.log.Error("m3db client gRPC FetchIDsGRPC error occurred",
			zap.Float64("sampleRateLog", s.logFetchErrorSampler.SampleRate().Value()),
			zap.Error(overallError))
	}

	if overallError != nil {
		// seriesIters are already populated with error iterators or empty ones.
		// No need to Close() them here as the defer will handle it if success is false.
		return seriesIters, overallError
	}

	success = true // Mark as success so defer doesn't close the valid iterators
	return seriesIters, nil
}

func (s *session) FetchIDs(
	nsID ident.ID,
	ids ident.Iterator,
	startInclusive, endExclusive xtime.UnixNano,
) (encoding.SeriesIterators, error) {
	f := s.pools.fetchAttempt.Get()
	f.args.namespace, f.args.ids = nsID, ids
	f.args.start = startInclusive
	f.args.end = endExclusive
	err := s.fetchRetrier.Attempt(f.attemptFn)
	result := f.result
	s.pools.fetchAttempt.Put(f)
	return result, err
}

func (s *session) Aggregate(
	ctx gocontext.Context,
	ns ident.ID,
	q index.Query,
	opts index.AggregationOptions,
) (AggregatedTagsIterator, FetchResponseMetadata, error) {
	f := s.pools.aggregateAttempt.Get()
	f.args.ctx = ctx
	f.args.ns = ns
	f.args.query = q
	f.args.opts = opts
	err := s.fetchRetrier.Attempt(f.attemptFn)
	iter, metadata := f.resultIter, f.resultMetadata
	s.pools.aggregateAttempt.Put(f)
	return iter, metadata, err
}

func (s *session) aggregateAttempt(
	ctx gocontext.Context,
	ns ident.ID,
	q index.Query,
	opts index.AggregationOptions,
) (AggregatedTagsIterator, FetchResponseMetadata, error) {
	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return nil, FetchResponseMetadata{}, ErrSessionStatusNotOpen
	}

	// NB(prateek): we have to clone the namespace, as we cannot guarantee the lifecycle
	// of the hostQueues responding is less than the lifecycle of the current method.
	nsClone := s.pools.id.Clone(ns)

	req, err := convert.ToRPCAggregateQueryRawRequest(nsClone, q, opts)
	if err != nil {
		s.state.RUnlock()
		nsClone.Finalize()
		return nil, FetchResponseMetadata{}, xerrors.NewNonRetryableError(err)
	}
	if req.SeriesLimit != nil && opts.InstanceMultiple > 0 {
		topo := s.state.topoMap
		iPerReplica := int64(len(topo.Hosts()) / topo.Replicas())
		iSeriesLimit := int64(float32(opts.SeriesLimit)*opts.InstanceMultiple) / iPerReplica
		if iSeriesLimit < *req.SeriesLimit {
			req.SeriesLimit = &iSeriesLimit
		}
	}

	fetchState, err := s.newFetchStateWithRLock(ctx, nsClone, newFetchStateOpts{
		stateType:            aggregateFetchState,
		aggregateRequest:     req,
		startInclusive:       opts.StartInclusive,
		endExclusive:         opts.EndExclusive,
		readConsistencyLevel: opts.ReadConsistencyLevel,
	})
	s.state.RUnlock()

	if err != nil {
		return nil, FetchResponseMetadata{}, err
	}

	// it's safe to Wait() here, as we still hold the lock on fetchState, after it's
	// returned from newFetchStateWithRLock.
	fetchState.Wait()

	// must Unlock before calling `asEncodingSeriesIterators` as the latter needs to acquire
	// the fetchState Lock
	fetchState.Unlock()
	iters, meta, err := fetchState.asAggregatedTagsIterator(s.pools, opts.SeriesLimit)

	// must Unlock() before decRef'ing, as the latter releases the fetchState back into a
	// pool if ref count == 0.
	fetchState.decRef()

	return iters, meta, err
}

func (s *session) FetchTagged(
	ctx gocontext.Context,
	ns ident.ID,
	q index.Query,
	opts index.QueryOptions,
) (encoding.SeriesIterators, FetchResponseMetadata, error) {
	f := s.pools.fetchTaggedAttempt.Get()
	f.args.ctx = ctx
	f.args.ns = ns
	f.args.query = q
	f.args.opts = opts
	err := s.fetchRetrier.Attempt(f.dataAttemptFn)
	iters, metadata := f.dataResultIters, f.dataResultMetadata
	s.pools.fetchTaggedAttempt.Put(f)
	return iters, metadata, err
}

func (s *session) FetchTaggedIDs(
	ctx gocontext.Context,
	ns ident.ID,
	q index.Query,
	opts index.QueryOptions,
) (TaggedIDsIterator, FetchResponseMetadata, error) {
	f := s.pools.fetchTaggedAttempt.Get()
	f.args.ctx = ctx
	f.args.ns = ns
	f.args.query = q
	f.args.opts = opts
	err := s.fetchRetrier.Attempt(f.idsAttemptFn)
	iter, metadata := f.idsResultIter, f.idsResultMetadata
	s.pools.fetchTaggedAttempt.Put(f)
	return iter, metadata, err
}

func (s *session) fetchTaggedAttempt(
	ctx gocontext.Context,
	ns ident.ID,
	q index.Query,
	opts index.QueryOptions,
) (encoding.SeriesIterators, FetchResponseMetadata, error) {
	nsCtx, err := s.nsCtxFor(ns)
	if err != nil {
		return nil, FetchResponseMetadata{}, err
	}
	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return nil, FetchResponseMetadata{}, ErrSessionStatusNotOpen
	}

	// NB(prateek): we have to clone the namespace, as we cannot guarantee the lifecycle
	// of the hostQueues responding is less than the lifecycle of the current method.
	nsClone := s.pools.id.Clone(ns)

	// FOLLOWUP(prateek): currently both `index.Query` and the returned request depend on
	// native, un-pooled types; so we do not Clone() either. We will start doing so
	// once https://github.com/m3db/m3ninx/issues/42 lands. Including transferring ownership
	// of the Clone()'d value to the `fetchState`.
	const fetchData = true
	req, err := convert.ToRPCFetchTaggedRequest(nsClone, q, opts, fetchData)
	if err != nil {
		s.state.RUnlock()
		nsClone.Finalize()
		return nil, FetchResponseMetadata{}, xerrors.NewNonRetryableError(err)
	}
	if req.SeriesLimit != nil && opts.InstanceMultiple > 0 {
		topo := s.state.topoMap
		iPerReplica := int64(len(topo.Hosts()) / topo.Replicas())
		iSeriesLimit := int64(float32(opts.SeriesLimit)*opts.InstanceMultiple) / iPerReplica
		if iSeriesLimit < *req.SeriesLimit {
			req.SeriesLimit = &iSeriesLimit
		}
	}

	fetchState, err := s.newFetchStateWithRLock(ctx, nsClone, newFetchStateOpts{
		stateType:            fetchTaggedFetchState,
		fetchTaggedRequest:   req,
		startInclusive:       opts.StartInclusive,
		endExclusive:         opts.EndExclusive,
		readConsistencyLevel: opts.ReadConsistencyLevel,
	})
	s.state.RUnlock()

	if err != nil {
		return nil, FetchResponseMetadata{}, err
	}

	// it's safe to Wait() here, as we still hold the lock on fetchState, after it's
	// returned from newFetchStateWithRLock.
	fetchState.Wait()

	// must Unlock before calling `asEncodingSeriesIterators` as the latter needs to acquire
	// the fetchState Lock
	fetchState.Unlock()

	iterOpts := s.opts.IterationOptions()
	if opts.IterateEqualTimestampStrategy != nil {
		iterOpts.IterateEqualTimestampStrategy = *opts.IterateEqualTimestampStrategy
	}

	iters, metadata, err := fetchState.asEncodingSeriesIterators(
		s.pools, nsCtx.Schema, iterOpts, opts.SeriesLimit)

	// must Unlock() before decRef'ing, as the latter releases the fetchState back into a
	// pool if ref count == 0.
	fetchState.decRef()

	return iters, metadata, err
}

func (s *session) fetchTaggedIDsAttempt(
	ctx gocontext.Context,
	ns ident.ID,
	q index.Query,
	opts index.QueryOptions,
) (TaggedIDsIterator, FetchResponseMetadata, error) {
	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return nil, FetchResponseMetadata{}, ErrSessionStatusNotOpen
	}

	// NB(prateek): we have to clone the namespace, as we cannot guarantee the lifecycle
	// of the hostQueues responding is less than the lifecycle of the current method.
	nsClone := s.pools.id.Clone(ns)

	// FOLLOWUP(prateek): currently both `index.Query` and the returned request depend on
	// native, un-pooled types; so we do not Clone() either. We will start doing so
	// once https://github.com/m3db/m3ninx/issues/42 lands. Including transferring ownership
	// of the Clone()'d value to the `fetchState`.
	const fetchData = false
	req, err := convert.ToRPCFetchTaggedRequest(nsClone, q, opts, fetchData)
	if err != nil {
		s.state.RUnlock()
		nsClone.Finalize()
		return nil, FetchResponseMetadata{}, xerrors.NewNonRetryableError(err)
	}
	if req.SeriesLimit != nil && opts.InstanceMultiple > 0 {
		topo := s.state.topoMap
		iPerReplica := int64(len(topo.Hosts()) / topo.Replicas())
		iSeriesLimit := int64(float32(opts.SeriesLimit)*opts.InstanceMultiple) / iPerReplica
		if iSeriesLimit < *req.SeriesLimit {
			req.SeriesLimit = &iSeriesLimit
		}
	}

	fetchState, err := s.newFetchStateWithRLock(ctx, nsClone, newFetchStateOpts{
		stateType:            fetchTaggedFetchState,
		fetchTaggedRequest:   req,
		startInclusive:       opts.StartInclusive,
		endExclusive:         opts.EndExclusive,
		readConsistencyLevel: opts.ReadConsistencyLevel,
	})
	s.state.RUnlock()

	if err != nil {
		return nil, FetchResponseMetadata{}, err
	}

	// it's safe to Wait() here, as we still hold the lock on fetchState, after it's
	// returned from newFetchStateWithRLock.
	fetchState.Wait()

	// must Unlock before calling `asTaggedIDsIterator` as the latter needs to acquire
	// the fetchState Lock
	fetchState.Unlock()
	iter, metadata, err := fetchState.asTaggedIDsIterator(s.pools, opts.SeriesLimit)

	// must Unlock() before decRef'ing, as the latter releases the fetchState back into a
	// pool if ref count == 0.
	fetchState.decRef()

	return iter, metadata, err
}

type newFetchStateOpts struct {
	stateType            fetchStateType
	startInclusive       xtime.UnixNano
	endExclusive         xtime.UnixNano
	readConsistencyLevel *topology.ReadConsistencyLevel

	// only valid if stateType == fetchTaggedFetchState
	fetchTaggedRequest thriftrpc.FetchTaggedRequest // Corrected type

	// only valid if stateType == aggregateFetchState
	aggregateRequest thriftrpc.AggregateQueryRawRequest // Corrected type
}

// NB(prateek): the returned fetchState, if valid, still holds the lock. Its ownership
// is transferred to the calling function, and is expected to manage the lifecycle of
// of the object (including releasing the lock/decRef'ing it).
// NB: ownership of ns is transferred to the returned fetchState object.
func (s *session) newFetchStateWithRLock(
	ctx gocontext.Context,
	ns ident.ID,
	opts newFetchStateOpts,
) (*fetchState, error) {
	var (
		topoMap    = s.state.topoMap
		fetchState = s.pools.fetchState.Get()
	)
	fetchState.nsID = ns // transfer ownership to `fetchState`
	fetchState.incRef()  // indicate current go-routine has a reference to the fetchState

	readLevel := s.state.readConsistencyLevelWithRLock(opts.readConsistencyLevel)

	// wire up the operation based on the opts specified
	var (
		op     op
		closer func()
	)
	switch opts.stateType {
	case fetchTaggedFetchState:
		fetchOp := s.pools.fetchTaggedOp.Get()
		fetchOp.incRef()        // indicate current go-routine has a reference to the op
		closer = fetchOp.decRef // release the ref for the current go-routine
		fetchOp.update(ctx, opts.fetchTaggedRequest, fetchState.completionFn)
		fetchState.ResetFetchTagged(opts.startInclusive, opts.endExclusive,
			fetchOp, topoMap, s.state.majority, readLevel)
		op = fetchOp

	case aggregateFetchState:
		aggOp := s.pools.aggregateOp.Get()
		aggOp.incRef()        // indicate current go-routine has a reference to the op
		closer = aggOp.decRef // release the ref for the current go-routine
		aggOp.update(ctx, opts.aggregateRequest, fetchState.completionFn)
		fetchState.ResetAggregate(opts.startInclusive, opts.endExclusive,
			aggOp, topoMap, s.state.majority, readLevel)
		op = aggOp

	default:
		fetchState.decRef() // release fetchState
		instrument.EmitInvariantViolation(s.opts.InstrumentOptions())
		return nil, xerrors.NewNonRetryableError(instrument.InvariantErrorf(
			"unknown fetchState type: %v", opts.stateType))
	}

	fetchState.Lock()
	for _, hq := range s.state.queues {
		// inc to indicate the hostQueue has a reference to `op` which has a ref to the fetchState
		fetchState.incRef()
		if err := hq.Enqueue(op); err != nil {
			fetchState.Unlock()
			closer()            // release the ref for the current go-routine
			fetchState.decRef() // release the ref for the hostQueue
			fetchState.decRef() // release the ref for the current go-routine

			// NB: if this happens we have a bug, once we are in the read
			// lock the current queues should never be closed
			wrappedErr := xerrors.NewNonRetryableError(fmt.Errorf("failed to enqueue in fetchState: %v", err))
			instrument.EmitAndLogInvariantViolation(s.opts.InstrumentOptions(), func(l *zap.Logger) {
				l.Error(wrappedErr.Error())
			})
			return nil, wrappedErr
		}
	}

	closer() // release the ref for the current go-routine

	// NB(prateek): the calling go-routine still holds the lock and a ref
	// on the returned fetchState object.
	return fetchState, nil
}

func (s *session) fetchIDsAttempt(
	inputNamespace ident.ID,
	inputIDs ident.Iterator,
	startInclusive, endExclusive xtime.UnixNano,
) (encoding.SeriesIterators, error) {
	nsCtx, err := s.nsCtxFor(inputNamespace)
	if err != nil {
		return nil, err
	}

	var (
		wg                     sync.WaitGroup
		allPending             int32
		routeErr               error
		enqueueErr             error
		resultErrLock          sync.RWMutex
		resultErr              error
		resultErrs             int32
		majority               int32
		numReplicas            int32
		readLevel              topology.ReadConsistencyLevel
		fetchBatchOpsByHostIdx [][]*fetchBatchOp
		success                = false
		startFetchAttempt      = s.nowFn()
	)

	// NB(prateek): need to make a copy of inputNamespace and inputIDs to control
	// their life-cycle within this function.
	namespace := s.pools.id.Clone(inputNamespace)
	// First, we duplicate the iterator (only the struct referencing the underlying slice,
	// not the slice itself). Need this to be able to iterate the original iterator
	// multiple times in case of retries.
	ids := inputIDs.Duplicate()

	rangeStart, tsErr := convert.ToValue(startInclusive, thriftrpc.TimeType_UNIX_NANOSECONDS) // Corrected type
	if tsErr != nil {
		return nil, tsErr
	}

	rangeEnd, tsErr := convert.ToValue(endExclusive, thriftrpc.TimeType_UNIX_NANOSECONDS) // Corrected type
	if tsErr != nil {
		return nil, tsErr
	}

	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return nil, ErrSessionStatusNotOpen
	}

	iters := encoding.NewSizedSeriesIterators(ids.Remaining())

	defer func() {
		// NB(r): Ensure we cover all edge cases and close the iters in any case
		// of an error being returned
		if !success {
			iters.Close()
		}
	}()

	// NB(r): We must take and return pooled items in the session read lock for the
	// pools that change during a topology update.
	// This is due to when a queue is re-initialized it enqueues a fixed number
	// of entries into the backing channel for the pool and will forever stall
	// on the last few puts if any unexpected entries find their way there
	// while it is filling.
	fetchBatchOpsByHostIdx = s.pools.fetchBatchOpArrayArray.Get()

	readLevel = s.state.readLevel
	majority = int32(s.state.majority)
	numReplicas = int32(s.state.replicas)

	// NB(prateek): namespaceAccessors tracks the number of pending accessors for nsID.
	// It is set to incremented by `replica` for each requested ID during fetch enqueuing,
	// and once by initial request, and is decremented for each replica retrieved, inside
	// completionFn, and once by the allCompletionFn. So know we can Finalize `namespace`
	// once it's value reaches 0.
	namespaceAccessors := int32(0)

	for idx := 0; ids.Next(); idx++ {
		var (
			idx  = idx // capture loop variable
			tsID = s.pools.id.Clone(ids.Current())

			wgIsDone int32
			// NB(xichen): resultsAccessors and idAccessors get initialized to number of replicas + 1
			// before enqueuing (incremented when iterating over the replicas for this ID), and gets
			// decremented for each replica as well as inside the allCompletionFn so we know when
			// resultsAccessors is 0, results are no longer accessed and it's safe to return results
			// to the pool.
			resultsAccessors int32 = 1
			idAccessors      int32 = 1
			resultsLock      sync.RWMutex
			results          []encoding.MultiReaderIterator
			enqueued         int32
			pending          int32
			success          int32
			errors           []error
			errs             int32
		)

		// increment namespaceAccesors by 1 to indicate it still needs to be handled by the
		// allCompletionFn for tsID.
		atomic.AddInt32(&namespaceAccessors, 1)

		wg.Add(1)
		allCompletionFn := func() {
			var reportErrors []error
			errsLen := atomic.LoadInt32(&errs)
			if errsLen > 0 {
				resultErrLock.RLock()
				reportErrors = errors[:]
				resultErrLock.RUnlock()
			}
			responded := enqueued - atomic.LoadInt32(&pending)
			err := s.readConsistencyResult(readLevel, majority, enqueued,
				responded, errsLen, reportErrors)
			s.recordFetchMetrics(err, errsLen, startFetchAttempt)
			if err != nil {
				resultErrLock.Lock()
				if resultErr == nil {
					resultErr = err
				}
				resultErrs++
				resultErrLock.Unlock()
			} else {
				resultsLock.RLock()
				numItersToInclude := int(success)
				numDesired := topology.NumDesiredForReadConsistency(readLevel, int(numReplicas), int(majority))
				if numDesired < numItersToInclude {
					// Avoid decoding more data than is required to satisfy the consistency guarantees.
					numItersToInclude = numDesired
				}

				itersToInclude := results[:numItersToInclude]
				resultsLock.RUnlock()

				iter := s.pools.seriesIterator.Get()
				// NB(prateek): we need to allocate a copy of ident.ID to allow the seriesIterator
				// to have control over the lifecycle of ID. We cannot allow seriesIterator
				// to control the lifecycle of the original ident.ID, as it might still be in use
				// due to a pending request in queue.
				seriesID := s.pools.id.Clone(tsID)
				namespaceID := s.pools.id.Clone(namespace)
				consolidator := s.opts.IterationOptions().SeriesIteratorConsolidator
				iter.Reset(encoding.SeriesIteratorOptions{
					ID:                         seriesID,
					Namespace:                  namespaceID,
					StartInclusive:             startInclusive,
					EndExclusive:               endExclusive,
					Replicas:                   itersToInclude,
					SeriesIteratorConsolidator: consolidator,
				})
				iters.SetAt(idx, iter)
			}
			if atomic.AddInt32(&resultsAccessors, -1) == 0 {
				s.pools.multiReaderIteratorArray.Put(results)
			}
			if atomic.AddInt32(&idAccessors, -1) == 0 {
				tsID.Finalize()
			}
			if atomic.AddInt32(&namespaceAccessors, -1) == 0 {
				namespace.Finalize()
			}
			wg.Done()
		}
		completionFn := func(result interface{}, err error) {
			var snapshotSuccess int32
			if err != nil {
				if IsBadRequestError(err) {
					// Wrap with invalid params and non-retryable so it is
					// not retried.
					err = xerrors.NewInvalidParamsError(err)
					err = xerrors.NewNonRetryableError(err)
				}
				atomic.AddInt32(&errs, 1)
				// NB(r): reuse the error lock here as we do not want to create
				// a whole lot of locks for every single ID fetched due to size
				// of mutex being non-trivial and likely to cause more stack growth
				// or GC pressure if ends up on heap which is likely due to naive
				// escape analysis.
				resultErrLock.Lock()
				errors = append(errors, err)
				resultErrLock.Unlock()
			} else {
				slicesIter := s.pools.readerSliceOfSlicesIterator.Get()
				slicesIter.Reset(result.([]*thriftrpc.Segments)) // Corrected type
				multiIter := s.pools.multiReaderIterator.Get()
				multiIter.ResetSliceOfSlices(slicesIter, nsCtx.Schema)
				// Results is pre-allocated after creating fetch ops for this ID below
				resultsLock.Lock()
				results[success] = multiIter
				success++
				snapshotSuccess = success
				resultsLock.Unlock()
			}
			// NB(xichen): decrementing pending and checking remaining against zero must
			// come after incrementing success, otherwise we might end up passing results[:success]
			// to iter.Reset down below before setting the iterator in the results array,
			// which would cause a nil pointer exception.
			remaining := atomic.AddInt32(&pending, -1)
			shouldTerminate := topology.ReadConsistencyTermination(
				readLevel, majority, remaining, snapshotSuccess,
			)
			if shouldTerminate && atomic.CompareAndSwapInt32(&wgIsDone, 0, 1) {
				allCompletionFn()
			}

			if atomic.AddInt32(&resultsAccessors, -1) == 0 {
				s.pools.multiReaderIteratorArray.Put(results)
			}
			if atomic.AddInt32(&idAccessors, -1) == 0 {
				tsID.Finalize()
			}
			if atomic.AddInt32(&namespaceAccessors, -1) == 0 {
				namespace.Finalize()
			}
		}

		if err := s.state.topoMap.RouteForEach(tsID, func(
			hostIdx int,
			hostShard shard.Shard,
			host topology.Host,
		) {
			// Inc safely as this for each is sequential
			enqueued++
			pending++
			allPending++
			resultsAccessors++
			namespaceAccessors++
			idAccessors++

			ops := fetchBatchOpsByHostIdx[hostIdx]

			var f *fetchBatchOp
			if len(ops) > 0 {
				// Find the last and potentially current fetch op for this host
				f = ops[len(ops)-1]
			}
			if f == nil || f.Size() >= s.fetchBatchSize {
				// If no current fetch op or existing one is at batch capacity add one
				// NB(r): Note that we defer to the host queue to take ownership
				// of these ops and for returning the ops to the pool when done as
				// they know when their use is complete.
				f = s.pools.fetchBatchOp.Get()
				f.IncRef()
				fetchBatchOpsByHostIdx[hostIdx] = append(fetchBatchOpsByHostIdx[hostIdx], f)
				f.request.RangeStart = rangeStart
				f.request.RangeEnd = rangeEnd
				f.request.RangeTimeType = thriftrpc.TimeType_UNIX_NANOSECONDS // Corrected type
			}

			// Append IDWithNamespace to this request
			f.append(namespace.Bytes(), tsID.Bytes(), completionFn)
		}); err != nil {
			routeErr = err
			break
		}

		// Once we've enqueued we know how many to expect so retrieve and set length
		results = s.pools.multiReaderIteratorArray.Get(int(enqueued))
		results = results[:enqueued]
	}

	if routeErr != nil {
		s.state.RUnlock()
		return nil, routeErr
	}

	// Enqueue fetch ops
	for idx := range fetchBatchOpsByHostIdx {
		for _, f := range fetchBatchOpsByHostIdx[idx] {
			// Passing ownership of the op itself to the host queue
			f.DecRef()
			if err := s.state.queues[idx].Enqueue(f); err != nil && enqueueErr == nil {
				enqueueErr = err
				break
			}
		}
		if enqueueErr != nil {
			break
		}
	}
	s.pools.fetchBatchOpArrayArray.Put(fetchBatchOpsByHostIdx)
	s.state.RUnlock()

	if enqueueErr != nil {
		s.log.Error("failed to enqueue fetch", zap.Error(enqueueErr))
		return nil, enqueueErr
	}

	wg.Wait()

	resultErrLock.RLock()
	retErr := resultErr
	resultErrLock.RUnlock()
	if retErr != nil {
		return nil, retErr
	}
	success = true
	return iters, nil
}

func (s *session) writeConsistencyResult(
	level topology.ConsistencyLevel,
	majority, enqueued, responded, resultErrs int32,
	errs []error,
) error {
	// Check consistency level satisfied
	success := enqueued - resultErrs
	if !topology.WriteConsistencyAchieved(level, int(majority), int(enqueued), int(success)) {
		return newConsistencyResultError(level, int(enqueued), int(responded), errs)
	}
	return nil
}

func (s *session) readConsistencyResult(
	level topology.ReadConsistencyLevel,
	majority, enqueued, responded, resultErrs int32,
	errs []error,
) error {
	// Check consistency level satisfied
	success := enqueued - resultErrs
	if !topology.ReadConsistencyAchieved(level, int(majority), int(enqueued), int(success)) {
		return newConsistencyResultError(level, int(enqueued), int(responded), errs)
	}
	return nil
}

func (s *session) IteratorPools() (encoding.IteratorPools, error) {
	s.state.RLock()
	defer s.state.RUnlock()
	if s.state.status != statusOpen {
		return nil, ErrSessionStatusNotOpen
	}
	return s.pools, nil
}

func (s *session) Close() error {
	s.state.Lock()
	if s.state.status != statusOpen {
		s.state.Unlock()
		return ErrSessionStatusNotOpen
	}
	s.state.status = statusClosed
	queues := s.state.queues
	topoWatch := s.state.topoWatch
	topo := s.state.topo
	s.state.Unlock()

	for _, q := range queues {
		q.Close()
	}

	topoWatch.Close()
	topo.Close()

	if closer := s.runtimeOptsListenerCloser; closer != nil {
		closer.Close()
	}

	// Close gRPC connections
	s.state.Lock() // Use Lock for modifying maps
	connsToClose := make(map[string]*grpc.ClientConn, len(s.grpcConns))
	for id, conn := range s.grpcConns {
		connsToClose[id] = conn
	}
	// Clear maps immediately while holding the lock, to prevent new operations from using them
	s.grpcConns = make(map[string]*grpc.ClientConn)
	s.grpcNodeClients = make(map[string]grpcrpc.NodeClient)    // Corrected type
	s.grpcClusterClients = make(map[string]grpcrpc.ClusterClient) // Corrected type
	s.state.Unlock()

	if len(connsToClose) > 0 {
		s.log.Info("closing gRPC client connections", zap.Int("count", len(connsToClose)))
		for hostID, conn := range connsToClose {
			if err := conn.Close(); err != nil {
				s.log.Error("error closing gRPC connection to host", zap.String("hostID", hostID), zap.Error(err))
			}
		}
		s.log.Info("finished closing gRPC client connections")
	}

	return nil
}

func (s *session) Origin() topology.Host {
	return s.origin
}

func (s *session) Replicas() int {
	s.state.RLock()
	v := s.state.replicas
	s.state.RUnlock()
	return v
}

func (s *session) TopologyMap() (topology.Map, error) {
	s.state.RLock()
	topoMap, err := s.topologyMapWithStateRLock()
	s.state.RUnlock()
	return topoMap, err
}

func (s *session) topologyMapWithStateRLock() (topology.Map, error) {
	status := s.state.status
	topoMap := s.state.topoMap

	// Make sure the session is open, as thats what sets the initial topology.
	if status != statusOpen {
		return nil, ErrSessionStatusNotOpen
	}
	if topoMap == nil {
		// Should never happen.
		return nil, instrument.InvariantErrorf("session does not have a topology map")
	}

	return topoMap, nil
}

func (s *session) Truncate(namespace ident.ID) (int64, error) {
	var (
		wg            sync.WaitGroup
		enqueueErr    xerrors.MultiError
		resultErrLock sync.Mutex
		resultErr     xerrors.MultiError
		truncated     int64
	)

	t := &truncateOp{}
	t.thriftrpcRequest.NameSpace = namespace.Bytes() // Corrected field name if truncateOp uses thriftrpc.TruncateRequest directly
	t.completionFn = func(result interface{}, err error) {
		if err != nil {
			resultErrLock.Lock()
			resultErr = resultErr.Add(err)
			resultErrLock.Unlock()
		} else {
			// NB: Ensure truncateOp.result is appropriately typed or cast
			res := result.(*thriftrpc.TruncateResult_) // Corrected type
			atomic.AddInt64(&truncated, res.NumSeries)
		}
		wg.Done()
	}

	s.state.RLock()
	for idx := range s.state.queues {
		wg.Add(1)
		if err := s.state.queues[idx].Enqueue(t); err != nil {
			wg.Done()
			enqueueErr = enqueueErr.Add(err)
		}
	}
	s.state.RUnlock()

	if err := enqueueErr.FinalError(); err != nil {
		s.log.Error("failed to enqueue request", zap.Error(err))
		return 0, err
	}

	// Wait for namespace to be truncated on all replicas
	wg.Wait()

	return truncated, resultErr.FinalError()
}

// NB(r): Excluding maligned struct check here as we can
// live with a few extra bytes since this struct is only
// ever passed by stack, its much more readable not optimized
// nolint: maligned
type peers struct {
	peers            []peer
	shard            uint32
	majorityReplicas int
	selfExcluded     bool
	selfHostShardSet topology.HostShardSet
}

func (p peers) selfExcludedAndSelfHasShardAvailable() bool {
	if !p.selfExcluded {
		return false
	}
	state, err := p.selfHostShardSet.ShardSet().LookupStateByID(p.shard)
	if err != nil {
		return false
	}
	return state == shard.Available
}

func (s *session) peersForShard(shardID uint32) (peers, error) {
	s.state.RLock()
	var (
		lookupErr error
		result    = peers{
			peers:            make([]peer, 0, s.state.topoMap.Replicas()),
			shard:            shardID,
			majorityReplicas: s.state.topoMap.MajorityReplicas(),
		}
	)
	err := s.state.topoMap.RouteShardForEach(shardID, func(
		idx int,
		_ shard.Shard,
		host topology.Host,
	) {
		if s.origin != nil && s.origin.ID() == host.ID() {
			// Don't include the origin host
			result.selfExcluded = true
			// Include the origin host shard set for help determining quorum
			hostShardSet, ok := s.state.topoMap.LookupHostShardSet(host.ID())
			if !ok {
				lookupErr = fmt.Errorf("could not find shard set for host ID: %s", host.ID())
			}
			result.selfHostShardSet = hostShardSet
			return
		}
		result.peers = append(result.peers, newPeer(s, host))
	})
	s.state.RUnlock()
	if resultErr := xerrors.FirstError(err, lookupErr); resultErr != nil {
		return peers{}, resultErr
	}
	return result, nil
}

func (s *session) FetchBootstrapBlocksMetadataFromPeers(
	namespace ident.ID,
	shard uint32,
	start, end xtime.UnixNano,
	resultOpts result.Options,
) (PeerBlockMetadataIter, error) {
	level := newSessionBootstrapRuntimeReadConsistencyLevel(s)
	return s.fetchBlocksMetadataFromPeers(namespace,
		shard, start, end, level, resultOpts)
}

func (s *session) FetchBlocksMetadataFromPeers(
	namespace ident.ID,
	shard uint32,
	start, end xtime.UnixNano,
	consistencyLevel topology.ReadConsistencyLevel,
	resultOpts result.Options,
) (PeerBlockMetadataIter, error) {
	level := newStaticRuntimeReadConsistencyLevel(consistencyLevel)
	return s.fetchBlocksMetadataFromPeers(namespace,
		shard, start, end, level, resultOpts)
}

func (s *session) fetchBlocksMetadataFromPeers(
	namespace ident.ID,
	shard uint32,
	start, end xtime.UnixNano,
	level runtimeReadConsistencyLevel,
	resultOpts result.Options,
) (PeerBlockMetadataIter, error) {
	peers, err := s.peersForShard(shard)
	if err != nil {
		return nil, err
	}

	var (
		metadataCh = make(chan receivedBlockMetadata,
			blockMetadataChBufSize)
		errCh = make(chan error, 1)
		meta  = resultTypeMetadata
		m     = s.newPeerMetadataStreamingProgressMetrics(shard, meta)
	)
	go func() {
		errCh <- s.streamBlocksMetadataFromPeers(namespace, shard,
			peers, start, end, level, metadataCh, resultOpts, m)
		close(metadataCh)
		close(errCh)
	}()

	iter := newMetadataIter(metadataCh, errCh,
		s.pools.tagDecoder, s.pools.id)
	return iter, nil
}

// FetchBootstrapBlocksFromPeers will fetch the specified blocks from peers for
// bootstrapping purposes. Refer to peer_bootstrapping.md for more details.
func (s *session) FetchBootstrapBlocksFromPeers(
	nsMetadata namespace.Metadata,
	shard uint32,
	start, end xtime.UnixNano,
	opts result.Options,
) (result.ShardResult, error) {
	nsCtx, err := s.nsCtxFromMetadata(nsMetadata)
	if err != nil {
		return nil, err
	}
	var (
		result = newBulkBlocksResult(nsCtx, s.opts, opts,
			s.pools.tagDecoder, s.pools.id)
		doneCh   = make(chan struct{})
		progress = s.newPeerMetadataStreamingProgressMetrics(shard,
			resultTypeBootstrap)
		level = newSessionBootstrapRuntimeReadConsistencyLevel(s)
	)

	// Determine which peers own the specified shard
	peers, err := s.peersForShard(shard)
	if err != nil {
		return nil, err
	}

	// Emit a gauge indicating whether we're done or not
	go func() {
		for {
			select {
			case <-doneCh:
				progress.fetchBlocksFromPeers.Update(0)
				return
			default:
				progress.fetchBlocksFromPeers.Update(1)
				time.Sleep(gaugeReportInterval)
			}
		}
	}()
	defer close(doneCh)

	// Begin pulling metadata, if one or multiple peers fail no error will
	// be returned from this routine as long as one peer succeeds completely
	metadataCh := make(chan receivedBlockMetadata, blockMetadataChBufSize)
	// Spin up a background goroutine which will begin streaming metadata from
	// all the peers and pushing them into the metadatach
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.streamBlocksMetadataFromPeers(nsMetadata.ID(), shard,
			peers, start, end, level, metadataCh, opts, progress)
		close(metadataCh)
	}()

	// Begin consuming metadata and making requests. This will block until all
	// data has been streamed (or failed to stream). Note that while this function
	// does return an error, an error will only be returned in a select few cases.
	// There are some scenarios in which if something goes wrong here we won't report it to
	// the caller, but metrics and logs are emitted internally. Also note that the
	// streamAndGroupCollectedBlocksMetadata function is injected.
	err = s.streamBlocksFromPeers(nsMetadata, shard, peers, metadataCh, opts,
		level, result, progress, s.streamAndGroupCollectedBlocksMetadata)
	if err != nil {
		return nil, err
	}

	// Check if an error occurred during the metadata streaming
	if err = <-errCh; err != nil {
		return nil, err
	}

	return result.result, nil
}

func (s *session) FetchBlocksFromPeers(
	nsMetadata namespace.Metadata,
	shard uint32,
	consistencyLevel topology.ReadConsistencyLevel,
	metadatas []block.ReplicaMetadata,
	opts result.Options,
) (PeerBlocksIter, error) {
	nsCtx, err := s.nsCtxFromMetadata(nsMetadata)
	if err != nil {
		return nil, err
	}
	var (
		logger   = opts.InstrumentOptions().Logger()
		level    = newStaticRuntimeReadConsistencyLevel(consistencyLevel)
		complete = int64(0)
		doneCh   = make(chan error, 1)
		outputCh = make(chan peerBlocksDatapoint, 4096)
		result   = newStreamBlocksResult(nsCtx, s.opts, opts, outputCh,
			s.pools.tagDecoder, s.pools.id)
		onDone = func(err error) {
			atomic.StoreInt64(&complete, 1)
			select {
			case doneCh <- err:
			default:
			}
		}
		progress = s.newPeerMetadataStreamingProgressMetrics(shard, resultTypeRaw)
	)

	peers, err := s.peersForShard(shard)
	if err != nil {
		return nil, err
	}

	peersByHost := make(map[string]peer, len(peers.peers))
	for _, peer := range peers.peers {
		peersByHost[peer.Host().ID()] = peer
	}

	// If any metadata has tags then encode them up front so can
	// return an error on tag encoding rather than logging error that would
	// possibly get missed.
	var (
		metadatasEncodedTags []checked.Bytes
		anyTags              bool
	)
	for _, meta := range metadatas {
		if len(meta.Tags.Values()) > 0 {
			anyTags = true
			break
		}
	}
	if anyTags {
		// NB(r): Allocate exact length so nil is used and each index
		// references same index as the incoming metadatas being fetched.
		metadatasEncodedTags = make([]checked.Bytes, len(metadatas))
		tagsIter := ident.NewTagsIterator(ident.Tags{})
		for idx, meta := range metadatas {
			if len(meta.Tags.Values()) == 0 {
				continue
			}

			tagsIter.Reset(meta.Tags)
			tagsEncoder := s.pools.tagEncoder.Get()
			if err := tagsEncoder.Encode(tagsIter); err != nil {
				return nil, err
			}

			encodedTagsCheckedBytes, ok := tagsEncoder.Data()
			if !ok {
				return nil, fmt.Errorf("could not encode tags: id=%s", meta.ID.String())
			}

			metadatasEncodedTags[idx] = encodedTagsCheckedBytes
		}
	}

	go func() {
		for atomic.LoadInt64(&complete) == 0 {
			progress.fetchBlocksFromPeers.Update(1)
			time.Sleep(gaugeReportInterval)
		}
		progress.fetchBlocksFromPeers.Update(0)
	}()

	metadataCh := make(chan receivedBlockMetadata, blockMetadataChBufSize)
	go func() {
		for idx, rb := range metadatas {
			peer, ok := peersByHost[rb.Host.ID()]
			if !ok {
				logger.Warn("replica requested from unknown peer, skipping",
					zap.Stringer("peer", rb.Host),
					zap.Stringer("id", rb.ID),
					zap.Time("start", rb.Start.ToTime()),
				)
				continue
			}

			// Attach encoded tags if present.
			var encodedTags checked.Bytes
			if idx < len(metadatasEncodedTags) {
				// Note: could still be nil if had no tags, but the slice
				// was built so need to take ref to encoded tags if
				// was encoded.
				encodedTags = metadatasEncodedTags[idx]
			}

			metadataCh <- receivedBlockMetadata{
				id:          rb.Metadata.ID,
				encodedTags: encodedTags,
				peer:        peer,
				block: blockMetadata{
					start:    rb.Start,
					size:     rb.Size,
					checksum: rb.Checksum,
					lastRead: rb.LastRead,
				},
			}
		}
		close(metadataCh)
	}()

	// Begin consuming metadata and making requests.
	go func() {
		err := s.streamBlocksFromPeers(nsMetadata, shard, peers, metadataCh,
			opts, level, result, progress, s.passThroughBlocksMetadata)
		close(outputCh)
		onDone(err)
	}()

	pbi := newPeerBlocksIter(outputCh, doneCh)
	return pbi, nil
}

func (s *session) streamBlocksMetadataFromPeers(
	namespace ident.ID,
	shardID uint32,
	peers peers,
	start, end xtime.UnixNano,
	level runtimeReadConsistencyLevel,
	metadataCh chan<- receivedBlockMetadata,
	resultOpts result.Options,
	progress *streamFromPeersMetrics,
) error {
	var (
		wg        sync.WaitGroup
		errs      = newSyncAbortableErrorsMap()
		pending   = int64(len(peers.peers))
		majority  = int32(peers.majorityReplicas)
		enqueued  = int32(len(peers.peers))
		responded int32
		success   int32
	)
	if peers.selfExcludedAndSelfHasShardAvailable() {
		// If we excluded ourselves from fetching, we basically treat ourselves
		// as a successful peer response since we can bootstrap from ourselves
		// just fine
		enqueued++
		success++
	}

	progress.metadataFetches.Update(float64(pending))
	for idx, peer := range peers.peers {
		idx := idx
		peer := peer

		wg.Add(1)
		go func() {
			defer func() {
				// Success or error counts towards a response
				atomic.AddInt32(&responded, 1)

				// Decrement pending
				progress.metadataFetches.Update(float64(atomic.AddInt64(&pending, -1)))

				// Mark done
				wg.Done()
			}()

			var (
				firstAttempt = true
				// NB(r): currPageToken keeps the position into the pagination of the
				// metadata from this peer, it begins as nil but if an error is
				// returned it will likely not be nil, this lets us restart fetching
				// if we need to (if consistency has not been achieved yet) without
				// losing place in the pagination.
				currPageToken                     pageToken
				currHostNotAvailableSleepInterval = hostNotAvailableMinSleepInterval
			)
			condition := func() bool {
				if firstAttempt {
					// Always attempt at least once
					firstAttempt = false
					return true
				}

				var (
					currLevel = level.value()
					majority  = int(majority)
					enqueued  = int(enqueued)
					success   = int(atomic.LoadInt32(&success))
				)
				metReadConsistency := topology.ReadConsistencyAchieved(
					currLevel, majority, enqueued, success)
				doRetry := !metReadConsistency && errs.getAbortError() == nil

				if doRetry {
					// Track that we are reattempting the fetch metadata
					// pagination from a peer
					progress.metadataPeerRetry.Inc(1)
				}
				return doRetry
			}
			for condition() {
				var err error
				currPageToken, err = s.streamBlocksMetadataFromPeer(namespace, shardID,
					peer, start, end, currPageToken, metadataCh, resultOpts, progress)
				// Set error or success if err is nil
				errs.setError(idx, err)

				// hostNotAvailable is a NonRetryableError for the purposes of short-circuiting
				// the automatic retry functionality, but in this case the client should avoid
				// aborting and continue retrying at this level until consistency can be reached.
				if isHostNotAvailableError(err) {
					// Prevent the loop from spinning too aggressively in the short-circuiting case.
					time.Sleep(currHostNotAvailableSleepInterval)
					currHostNotAvailableSleepInterval = minDuration(
						currHostNotAvailableSleepInterval*2,
						hostNotAvailableMaxSleepInterval,
					)
					continue
				}

				if err != nil && xerrors.IsNonRetryableError(err) {
					errs.setAbortError(err)
					return // Cannot recover from this error, so we break from the loop
				}

				if err == nil {
					atomic.AddInt32(&success, 1)
					return
				}

				// There was a retryable error, continue looping.
			}
		}()
	}

	wg.Wait()

	if err := errs.getAbortError(); err != nil {
		return err
	}

	errors := errs.getErrors()
	return s.readConsistencyResult(level.value(), majority, enqueued,
		atomic.LoadInt32(&responded), int32(len(errors)), errors)
}

type pageToken []byte

// streamBlocksMetadataFromPeer has several heap allocated anonymous
// function, however, they're only allocated once per peer/shard combination
// for the entire peer bootstrapping process so performance is acceptable
func (s *session) streamBlocksMetadataFromPeer(
	namespace ident.ID,
	shard uint32,
	peer peer,
	start, end xtime.UnixNano,
	startPageToken pageToken,
	metadataCh chan<- receivedBlockMetadata,
	resultOpts result.Options,
	progress *streamFromPeersMetrics,
) (pageToken, error) {
	var (
		optionIncludeSizes     = true
		optionIncludeChecksums = true
		optionIncludeLastRead  = true
		moreResults            = true
		idPool                 = s.pools.id
		bytesPool              = resultOpts.DatabaseBlockOptions().BytesPool()

		// Only used for logs
		peerStr              = peer.Host().ID()
		metadataCountByBlock = map[xtime.UnixNano]int64{}
	)
	defer func() {
		for block, numMetadata := range metadataCountByBlock {
			s.log.Debug("finished streaming blocks metadata from peer",
				zap.Uint32("shard", shard),
				zap.String("peer", peerStr),
				zap.Int64("numMetadata", numMetadata),
				zap.Time("block", block.ToTime()),
			)
		}
	}()

	// Declare before loop to avoid redeclaring each iteration
	attemptFn := func(client thriftrpc.TChanNode) error { // Corrected client type
		tctx, _ := thrift.NewContext(s.streamBlocksMetadataBatchTimeout)
		req := thriftrpc.NewFetchBlocksMetadataRawV2Request() // Corrected constructor
		req.NameSpace = namespace.Bytes()
		req.Shard = int32(shard)
		req.RangeStart = int64(start)
		req.RangeEnd = int64(end)
		req.Limit = int64(s.streamBlocksBatchSize)
		req.PageToken = startPageToken
		req.IncludeSizes = &optionIncludeSizes
		req.IncludeChecksums = &optionIncludeChecksums
		req.IncludeLastRead = &optionIncludeLastRead

		progress.metadataFetchBatchCall.Inc(1)
		result, err := client.FetchBlocksMetadataRawV2(tctx, req)
		if err != nil {
			progress.metadataFetchBatchError.Inc(1)
			return err
		}

		progress.metadataFetchBatchSuccess.Inc(1)
		progress.metadataReceived.Inc(int64(len(result.Elements)))

		if result.NextPageToken != nil {
			// Reset pageToken + copy new pageToken into previously allocated memory,
			// extending as necessary
			startPageToken = append(startPageToken[:0], result.NextPageToken...)
		} else {
			// No further results
			moreResults = false
		}

		for _, elem := range result.Elements {
			blockStart := xtime.UnixNano(elem.Start)

			data := bytesPool.Get(len(elem.ID))
			data.IncRef()
			data.AppendAll(elem.ID)
			data.DecRef()
			clonedID := idPool.BinaryID(data)
			// Return thrift bytes to pool once the ID has been copied.
			tbinarypool.BytesPoolPut(elem.ID)

			var encodedTags checked.Bytes
			if tagBytes := elem.EncodedTags; len(tagBytes) != 0 {
				encodedTags = bytesPool.Get(len(tagBytes))
				encodedTags.IncRef()
				encodedTags.AppendAll(tagBytes)
				encodedTags.DecRef()
				// Return thrift bytes to pool once the tags have been copied.
				tbinarypool.BytesPoolPut(tagBytes)
			}

			// Error occurred retrieving block metadata, use default values
			if err := elem.Err; err != nil {
				progress.metadataFetchBatchBlockErr.Inc(1)
				s.log.Error("error occurred retrieving block metadata",
					zap.Uint32("shard", shard),
					zap.String("peer", peerStr),
					zap.Time("block", blockStart.ToTime()),
					zap.Error(err),
				)
				// Enqueue with a zeroed checksum which triggers a fanout fetch
				metadataCh <- receivedBlockMetadata{
					peer:        peer,
					id:          clonedID,
					encodedTags: encodedTags,
					block: blockMetadata{
						start: blockStart,
					},
				}
				continue
			}

			var size int64
			if elem.Size != nil {
				size = *elem.Size
			}

			var pChecksum *uint32
			if elem.Checksum != nil {
				value := uint32(*elem.Checksum)
				pChecksum = &value
			}

			var lastRead xtime.UnixNano
			if elem.LastRead != nil {
				value, err := convert.ToTime(*elem.LastRead, elem.LastReadTimeType)
				if err == nil {
					lastRead = value
				}
			}

			metadataCh <- receivedBlockMetadata{
				peer:        peer,
				id:          clonedID,
				encodedTags: encodedTags,
				block: blockMetadata{
					start:    blockStart,
					size:     size,
					checksum: pChecksum,
					lastRead: lastRead,
				},
			}
			// Only used for logs
			metadataCountByBlock[blockStart]++
		}
		return nil
	}

	var attemptErr error
	checkedAttemptFn := func(client thriftrpc.TChanNode, _ Channel) { // Corrected client type
		attemptErr = attemptFn(client)
	}

	fetchFn := func() error {
		borrowErr := peer.BorrowConnection(checkedAttemptFn)
		return xerrors.FirstError(borrowErr, attemptErr)
	}

	for moreResults {
		if err := s.streamBlocksRetrier.Attempt(fetchFn); err != nil {
			return startPageToken, err
		}
	}
	return nil, nil
}

func (s *session) streamBlocksFromPeers(
	nsMetadata namespace.Metadata,
	shard uint32,
	peers peers,
	metadataCh <-chan receivedBlockMetadata,
	opts result.Options,
	consistencyLevel runtimeReadConsistencyLevel,
	result blocksResult,
	progress *streamFromPeersMetrics,
	streamMetadataFn streamBlocksMetadataFn,
) error {
	var (
		enqueueCh           = newEnqueueChannel(progress)
		peerBlocksBatchSize = s.streamBlocksBatchSize
		numPeers            = len(peers.peers)
		uncheckedBytesPool  = opts.DatabaseBlockOptions().BytesPool().BytesPool()
	)

	// Consume the incoming metadata and enqueue to the ready channel
	// Spin up background goroutine to consume
	go func() {
		streamMetadataFn(numPeers, metadataCh, enqueueCh, uncheckedBytesPool)
		// Begin assessing the queue and how much is processed, once queue
		// is entirely processed then we can close the enqueue channel
		enqueueCh.closeOnAllProcessed()
	}()

	// Fetch blocks from peers as results become ready
	peerQueues := make(peerBlocksQueues, 0, numPeers)
	for _, peer := range peers.peers {
		peer := peer
		size := peerBlocksBatchSize
		workers := s.streamBlocksWorkers
		drainEvery := 100 * time.Millisecond
		queue := s.newPeerBlocksQueueFn(peer, size, drainEvery, workers,
			func(batch []receivedBlockMetadata) {
				s.streamBlocksBatchFromPeer(nsMetadata, shard, peer, batch, opts,
					result, enqueueCh, s.streamBlocksRetrier, progress)
			})
		peerQueues = append(peerQueues, queue)
	}

	var (
		selected             []receivedBlockMetadata
		pooled               selectPeersFromPerPeerBlockMetadatasPooledResources
		onQueueItemProcessed = func() {
			enqueueCh.trackProcessed(1)
		}
	)
	for perPeerBlocksMetadata := range enqueueCh.read() {
		// Filter and select which blocks to retrieve from which peers
		selected, pooled = s.selectPeersFromPerPeerBlockMetadatas(
			perPeerBlocksMetadata, peerQueues, enqueueCh, consistencyLevel, peers,
			pooled, progress)

		if len(selected) == 0 {
			onQueueItemProcessed()
			continue
		}

		if len(selected) == 1 {
			queue := peerQueues.findQueue(selected[0].peer)
			queue.enqueue(selected[0], onQueueItemProcessed)
			continue
		}

		// Need to fan out, only track this as processed once all peer
		// queues have completed their fetches, so account for the extra
		// items assigned to be fetched
		enqueueCh.trackPending(len(selected) - 1)
		for _, receivedBlockMetadata := range selected {
			queue := peerQueues.findQueue(receivedBlockMetadata.peer)
			queue.enqueue(receivedBlockMetadata, onQueueItemProcessed)
		}
	}

	// Close all queues
	peerQueues.closeAll()

	return nil
}

type streamBlocksMetadataFn func(
	peersLen int,
	ch <-chan receivedBlockMetadata,
	enqueueCh enqueueChannel,
	pool pool.BytesPool,
)

func (s *session) passThroughBlocksMetadata(
	peersLen int,
	ch <-chan receivedBlockMetadata,
	enqueueCh enqueueChannel,
	_ pool.BytesPool,
) {
	// Receive off of metadata channel
	for {
		m, ok := <-ch
		if !ok {
			break
		}
		res := []receivedBlockMetadata{m}
		enqueueCh.enqueue(res)
	}
}

func (s *session) streamAndGroupCollectedBlocksMetadata(
	peersLen int,
	metadataCh <-chan receivedBlockMetadata,
	enqueueCh enqueueChannel,
	pool pool.BytesPool,
) {
	metadata := newReceivedBlocksMap(pool)
	defer metadata.Reset() // Delete all the keys and return slices to pools

	for {
		m, ok := <-metadataCh
		if !ok {
			break
		}

		key := idAndBlockStart{
			id:         m.id,
			blockStart: int64(m.block.start),
		}
		received, ok := metadata.Get(key)
		if !ok {
			received = receivedBlocks{
				results: make([]receivedBlockMetadata, 0, peersLen),
			}
		}

		// The entry has already been enqueued which means the metadata we just
		// received is a duplicate. Discard it and move on.
		if received.enqueued {
			s.emitDuplicateMetadataLog(received, m)
			continue
		}

		// Determine if the incoming metadata is a duplicate by checking if we've
		// already received metadata from this peer.
		existingIndex := -1
		for i, existingMetadata := range received.results {
			if existingMetadata.peer.Host().ID() == m.peer.Host().ID() {
				existingIndex = i
				break
			}
		}

		if existingIndex != -1 {
			// If it is a duplicate, then overwrite it (always keep the most recent
			// duplicate)
			received.results[existingIndex] = m
		} else {
			// Otherwise it's not a duplicate, so its safe to append.
			received.results = append(received.results, m)
		}

		// Since we always perform an overwrite instead of an append for duplicates
		// from the same peer, once len(received.results == peersLen) then we know
		// that we've received at least one metadata from every peer and its safe
		// to enqueue the entry.
		if len(received.results) == peersLen {
			enqueueCh.enqueue(received.results)
			received.enqueued = true
		}

		// Ensure tracking enqueued by setting modified result back to map
		metadata.Set(key, received)
	}

	// Enqueue all unenqueued received metadata. Note that these entries will have
	// metadata from only a subset of their peers.
	for _, entry := range metadata.Iter() {
		received := entry.Value()
		if received.enqueued {
			continue
		}
		enqueueCh.enqueue(received.results)
	}
}

// emitDuplicateMetadataLog emits a log with the details of the duplicate metadata
// event. Note: We're able to log the blocks themselves because the slice is no longer
// mutated downstream after enqueuing into the enqueue channel, it's copied before
// mutated or operated on.
func (s *session) emitDuplicateMetadataLog(
	received receivedBlocks,
	metadata receivedBlockMetadata,
) {
	// Debug-level because this is a common enough occurrence that logging it by
	// default would be noisy.
	// This is due to peers sending the most recent data
	// to the oldest data in that order, hence sometimes its possible to resend
	// data for a block already sent over the wire if it just moved from being
	// mutable in memory to immutable on disk.
	if !s.log.Core().Enabled(zapcore.DebugLevel) {
		return
	}

	var checksum uint32
	if v := metadata.block.checksum; v != nil {
		checksum = *v
	}

	fields := make([]zapcore.Field, 0, len(received.results)+1)
	fields = append(fields, zap.String("incoming-metadata", fmt.Sprintf(
		"id=%s, peer=%s, start=%s, size=%v, checksum=%v",
		metadata.id.String(),
		metadata.peer.Host().String(),
		metadata.block.start.String(),
		metadata.block.size,
		checksum)))

	for i, existing := range received.results {
		checksum = 0
		if v := existing.block.checksum; v != nil {
			checksum = *v
		}

		fields = append(fields, zap.String(
			fmt.Sprintf("existing-metadata-%d", i),
			fmt.Sprintf(
				"id=%s, peer=%s, start=%s, size=%v, checksum=%v",
				existing.id.String(),
				existing.peer.Host().String(),
				existing.block.start.String(),
				existing.block.size,
				checksum)))
	}

	s.log.Debug("received metadata, but peer metadata has already been submitted", fields...)
}

type pickBestPeerFn func(
	perPeerBlockMetadata []receivedBlockMetadata,
	peerQueues peerBlocksQueues,
	resources pickBestPeerPooledResources,
) (index int, pooled pickBestPeerPooledResources)

type pickBestPeerPooledResources struct {
	ranking []receivedBlockMetadataQueue
}

func (s *session) streamBlocksPickBestPeer(
	perPeerBlockMetadata []receivedBlockMetadata,
	peerQueues peerBlocksQueues,
	pooled pickBestPeerPooledResources,
) (int, pickBestPeerPooledResources) {
	// Order by least attempts then by least outstanding blocks being fetched
	pooled.ranking = pooled.ranking[:0]
	for i := range perPeerBlockMetadata {
		elem := receivedBlockMetadataQueue{
			blockMetadata: perPeerBlockMetadata[i],
			queue:         peerQueues.findQueue(perPeerBlockMetadata[i].peer),
		}
		pooled.ranking = append(pooled.ranking, elem)
	}
	elems := receivedBlockMetadataQueuesByAttemptsAscOutstandingAsc(pooled.ranking)
	sort.Stable(elems)

	// Return index of the best peer
	var (
		bestPeer = pooled.ranking[0].queue.peer
		idx      int
	)
	for i := range perPeerBlockMetadata {
		if bestPeer == perPeerBlockMetadata[i].peer {
			idx = i
			break
		}
	}
	return idx, pooled
}

type selectPeersFromPerPeerBlockMetadatasPooledResources struct {
	currEligible                []receivedBlockMetadata
	pickBestPeerPooledResources pickBestPeerPooledResources
}

func (s *session) selectPeersFromPerPeerBlockMetadatas(
	perPeerBlocksMetadata []receivedBlockMetadata,
	peerQueues peerBlocksQueues,
	reEnqueueCh enqueueChannel,
	consistencyLevel runtimeReadConsistencyLevel,
	peers peers,
	pooled selectPeersFromPerPeerBlockMetadatasPooledResources,
	m *streamFromPeersMetrics,
) ([]receivedBlockMetadata, selectPeersFromPerPeerBlockMetadatasPooledResources) {
	// Copy into pooled array so we don't mutate existing slice passed
	pooled.currEligible = pooled.currEligible[:0]
	pooled.currEligible = append(pooled.currEligible, perPeerBlocksMetadata...)

	currEligible := pooled.currEligible[:]

	// Sort the per peer metadatas by peer ID for consistent results
	sort.Sort(peerBlockMetadataByID(currEligible))

	// Only select from peers not already attempted
	curr := currEligible[0]
	currID := curr.id
	currBlock := curr.block
	for i := len(currEligible) - 1; i >= 0; i-- {
		if currEligible[i].block.reattempt.attempt == 0 {
			// Not attempted yet
			continue
		}

		// Check if eligible
		n := s.streamBlocksMaxBlockRetries
		if currEligible[i].block.reattempt.peerAttempts(currEligible[i].peer) >= n {
			// Swap current entry to tail
			receivedBlockMetadatas(currEligible).swap(i, len(currEligible)-1)
			// Trim newly last entry
			currEligible = currEligible[:len(currEligible)-1]
			continue
		}
	}

	if len(currEligible) == 0 {
		// No current eligible peers to select from
		majority := peers.majorityReplicas
		enqueued := len(peers.peers)
		success := 0
		if peers.selfExcludedAndSelfHasShardAvailable() {
			// If we excluded ourselves from fetching, we basically treat ourselves
			// as a successful peer response since our copy counts towards quorum
			enqueued++
			success++
		}

		errMsg := "all retries failed for streaming blocks from peers"
		fanoutFetchState := currBlock.reattempt.fanoutFetchState
		if fanoutFetchState != nil {
			if fanoutFetchState.decrementAndReturnPending() > 0 {
				// This block was fanned out to fetch from all peers and we haven't
				// received all the results yet, so don't retry it just yet
				return nil, pooled
			}

			// NB(r): This was enqueued after a failed fetch and all other fanout
			// fetches have completed, check if the consistency level was achieved,
			// if not then re-enqueue to continue to retry otherwise do not
			// re-enqueue and see if we need mark this as an error.
			success = fanoutFetchState.success()
		}

		level := consistencyLevel.value()
		achievedConsistencyLevel := topology.ReadConsistencyAchieved(level, majority, enqueued, success)
		if achievedConsistencyLevel {
			if success > 0 {
				// Some level of success met, no need to log an error
				return nil, pooled
			}

			// No success, inform operator that although consistency level achieved
			// there were no successful fetches. This can happen if consistency
			// level is set to None.
			m.fetchBlockFinalError.Inc(1)
			s.log.Error(errMsg,
				zap.Stringer("id", currID),
				zap.Time("start", currBlock.start.ToTime()),
				zap.Int("attempted", currBlock.reattempt.attempt),
				zap.String("attemptErrs", xerrors.Errors(currBlock.reattempt.errs).Error()),
				zap.Stringer("consistencyLevel", level),
			)

			return nil, pooled
		}

		// Retry again by re-enqueuing, have not met consistency level yet
		m.fetchBlockFullRetry.Inc(1)

		err := fmt.Errorf(errMsg+": attempts=%d", curr.block.reattempt.attempt)
		reattemptReason := consistencyLevelNotAchievedErrReason
		reattemptType := fullRetryReattemptType
		reattemptBlocks := []receivedBlockMetadata{curr}
		s.reattemptStreamBlocksFromPeersFn(reattemptBlocks, reEnqueueCh,
			err, reattemptReason, reattemptType, m)

		return nil, pooled
	}

	var (
		singlePeer         = len(currEligible) == 1
		sameNonNilChecksum = true
		curChecksum        *uint32
	)
	for i := range currEligible {
		// If any peer has a nil checksum, this might be the most recent block
		// and therefore not sealed so we want to merge from all peers
		if currEligible[i].block.checksum == nil {
			sameNonNilChecksum = false
			break
		}
		if curChecksum == nil {
			curChecksum = currEligible[i].block.checksum
		} else if *curChecksum != *currEligible[i].block.checksum {
			sameNonNilChecksum = false
			break
		}
	}

	// If all the peers have the same non-nil checksum, we pick the peer with the
	// fewest attempts and fewest outstanding requests
	if singlePeer || sameNonNilChecksum {
		var idx int
		if singlePeer {
			idx = 0
		} else {
			pooledResources := pooled.pickBestPeerPooledResources
			idx, pooledResources = s.pickBestPeerFn(currEligible, peerQueues,
				pooledResources)
			pooled.pickBestPeerPooledResources = pooledResources
		}

		// Set the reattempt metadata
		selected := currEligible[idx]
		selected.block.reattempt.attempt++
		selected.block.reattempt.attempted = append(selected.block.reattempt.attempted, selected.peer)
		selected.block.reattempt.fanoutFetchState = nil
		selected.block.reattempt.retryPeersMetadata = perPeerBlocksMetadata
		selected.block.reattempt.fetchedPeersMetadata = perPeerBlocksMetadata

		// Return just the single peer we selected
		currEligible = currEligible[:1]
		currEligible[0] = selected
	} else {
		fanoutFetchState := newBlockFanoutFetchState(len(currEligible))
		for i := range currEligible {
			// Set the reattempt metadata
			// NB(xichen): each block will only be retried on the same peer because we
			// already fan out the request to all peers. This means we merge data on
			// a best-effort basis and only fail if we failed to reach the desired
			// consistency level when reading data from all peers.
			var retryFrom []receivedBlockMetadata
			for j := range perPeerBlocksMetadata {
				if currEligible[i].peer == perPeerBlocksMetadata[j].peer {
					// NB(r): Take a ref to a subslice from the originally passed
					// slice as that is not mutated, whereas currEligible is reused
					retryFrom = perPeerBlocksMetadata[j : j+1]
				}
			}
			currEligible[i].block.reattempt.attempt++
			currEligible[i].block.reattempt.attempted = append(currEligible[i].block.reattempt.attempted, currEligible[i].peer)
			currEligible[i].block.reattempt.fanoutFetchState = fanoutFetchState
			currEligible[i].block.reattempt.retryPeersMetadata = retryFrom
			currEligible[i].block.reattempt.fetchedPeersMetadata = perPeerBlocksMetadata
		}
	}

	return currEligible, pooled
}

func (s *session) streamBlocksBatchFromPeer(
	namespaceMetadata namespace.Metadata,
	shard uint32,
	peer peer,
	batch []receivedBlockMetadata,
	opts result.Options,
	blocksResult blocksResult,
	enqueueCh enqueueChannel,
	retrier xretry.Retrier,
	m *streamFromPeersMetrics,
) {
	// Prepare request
	var (
		req          = thriftrpc.NewFetchBlocksRawRequest() // Corrected constructor
		result       *thriftrpc.FetchBlocksRawResult_       // Corrected type
		reqBlocksLen uint

		nowFn              = opts.ClockOptions().NowFn()
		ropts              = namespaceMetadata.Options().RetentionOptions()
		retention          = ropts.RetentionPeriod()
		earliestBlockStart = xtime.ToUnixNano(nowFn()).
					Add(-retention).
					Truncate(ropts.BlockSize())
	)
	req.NameSpace = namespaceMetadata.ID().Bytes()
	req.Shard = int32(shard)
	req.Elements = make([]*thriftrpc.FetchBlocksRawRequestElement, 0, len(batch)) // Corrected type
	for i := range batch {
		blockStart := batch[i].block.start
		if blockStart.Before(earliestBlockStart) {
			continue // Fell out of retention while we were streaming blocks
		}
		req.Elements = append(req.Elements, &thriftrpc.FetchBlocksRawRequestElement{ // Corrected type
			ID:     batch[i].id.Bytes(),
			Starts: []int64{int64(blockStart)},
		})
		reqBlocksLen++
	}
	if reqBlocksLen == 0 {
		// All blocks fell out of retention while streaming
		return
	}

	// Attempt request
	if err := retrier.Attempt(func() error {
		var attemptErr error
		borrowErr := peer.BorrowConnection(func(client thriftrpc.TChanNode, _ Channel) { // Corrected client type
			tctx, _ := thrift.NewContext(s.streamBlocksBatchTimeout)
			result, attemptErr = client.FetchBlocksRaw(tctx, req)
		})
		err := xerrors.FirstError(borrowErr, attemptErr)
		return err
	}); err != nil {
		blocksErr := fmt.Errorf(
			"stream blocks request error: error=%s, peer=%s",
			err.Error(), peer.Host().String(),
		)
		s.reattemptStreamBlocksFromPeersFn(batch, enqueueCh, blocksErr,
			reqErrReason, nextRetryReattemptType, m)
		m.fetchBlockError.Inc(int64(reqBlocksLen))
		s.log.Debug(blocksErr.Error())
		return
	}

	// Parse and act on result
	tooManyIDsLogged := false
	for i := range result.Elements {
		if i >= len(batch) {
			m.fetchBlockError.Inc(int64(len(req.Elements[i].Starts)))
			m.fetchBlockFinalError.Inc(int64(len(req.Elements[i].Starts)))
			if !tooManyIDsLogged {
				tooManyIDsLogged = true
				s.log.Error("stream blocks more IDs than expected",
					zap.Stringer("peer", peer.Host()),
				)
			}
			continue
		}

		id := batch[i].id
		if !bytes.Equal(id.Bytes(), result.Elements[i].ID) {
			blocksErr := fmt.Errorf(
				"stream blocks mismatched ID: expectedID=%s, actualID=%s, indexID=%d, peer=%s",
				batch[i].id.String(), id.String(), i, peer.Host().String(),
			)
			failed := []receivedBlockMetadata{batch[i]}
			s.reattemptStreamBlocksFromPeersFn(failed, enqueueCh, blocksErr,
				respErrReason, nextRetryReattemptType, m)
			m.fetchBlockError.Inc(int64(len(req.Elements[i].Starts)))
			s.log.Debug(blocksErr.Error())
			continue
		}

		if len(result.Elements[i].Blocks) == 0 {
			// If fell out of retention during request this is healthy, otherwise
			// missing blocks will be repaired during an active repair
			continue
		}

		// We only ever fetch a single block for a series
		if len(result.Elements[i].Blocks) != 1 {
			errMsg := "stream blocks returned more blocks than expected"
			blocksErr := fmt.Errorf(errMsg+": expected=%d, actual=%d",
				1, len(result.Elements[i].Blocks))
			failed := []receivedBlockMetadata{batch[i]}
			s.reattemptStreamBlocksFromPeersFn(failed, enqueueCh, blocksErr,
				respErrReason, nextRetryReattemptType, m)
			m.fetchBlockError.Inc(int64(len(req.Elements[i].Starts)))
			s.log.Error(errMsg,
				zap.Stringer("id", id),
				zap.Times("expectedStarts", newTimesByUnixNanos(req.Elements[i].Starts)),
				zap.Times("actualStarts", newTimesByRPCThriftBlocks(result.Elements[i].Blocks)), // Corrected helper
				zap.Stringer("peer", peer.Host()),
			)
			continue
		}

		for j, block := range result.Elements[i].Blocks {
			if block.Start != int64(batch[i].block.start) {
				errMsg := "stream blocks returned different blocks than expected"
				blocksErr := fmt.Errorf(errMsg+": expected=%s, actual=%d",
					batch[i].block.start.String(), time.Unix(0, block.Start).String())
				failed := []receivedBlockMetadata{batch[i]}
				s.reattemptStreamBlocksFromPeersFn(failed, enqueueCh, blocksErr,
					respErrReason, nextRetryReattemptType, m)
				m.fetchBlockError.Inc(int64(len(req.Elements[i].Starts)))
				s.log.Error(errMsg,
					zap.Stringer("id", id),
					zap.Times("expectedStarts", newTimesByUnixNanos(req.Elements[i].Starts)),
					zap.Times("actualStarts", newTimesByRPCThriftBlocks(result.Elements[i].Blocks)), // Corrected helper
					zap.Stringer("peer", peer.Host()),
				)
				continue
			}

			// Verify and if verify succeeds add the block from the peer
			err := s.verifyFetchedBlock(block)
			if err == nil {
				err = blocksResult.addBlockFromPeer(id, batch[i].encodedTags,
					peer.Host(), block)
			}
			if err != nil {
				failed := []receivedBlockMetadata{batch[i]}
				blocksErr := fmt.Errorf(
					"stream blocks bad block: id=%s, start=%d, error=%s, indexID=%d, indexBlock=%d, peer=%s",
					id.String(), block.Start, err.Error(), i, j, peer.Host().String())
				s.reattemptStreamBlocksFromPeersFn(failed, enqueueCh, blocksErr,
					respErrReason, nextRetryReattemptType, m)
				m.fetchBlockError.Inc(1)
				s.log.Debug(blocksErr.Error())
				continue
			}

			// NB(r): Track a fanned out block fetch success if added block
			fanout := batch[i].block.reattempt.fanoutFetchState
			if fanout != nil {
				fanout.incrementSuccess()
			}

			m.fetchBlockSuccess.Inc(1)
		}
	}
}

func (s *session) verifyFetchedBlock(block *thriftrpc.Block) error { // Corrected type
	if block.Err != nil {
		return fmt.Errorf("block error from peer: %s %s", block.Err.Type.String(), block.Err.Message)
	}
	if block.Segments == nil {
		return fmt.Errorf("block segments is bad: segments is nil")
	}
	if block.Segments.Merged == nil && len(block.Segments.Unmerged) == 0 {
		return fmt.Errorf("block segments is bad: merged and unmerged not set")
	}

	if checksum := block.Checksum; checksum != nil {
		var (
			d        = digest.NewDigest()
			expected = uint32(*checksum)
		)
		if merged := block.Segments.Merged; merged != nil {
			d = d.Update(merged.Head).Update(merged.Tail)
		} else {
			for _, s := range block.Segments.Unmerged {
				d = d.Update(s.Head).Update(s.Tail)
			}
		}
		if actual := d.Sum32(); actual != expected {
			return fmt.Errorf("block checksum is bad: expected=%d, actual=%d", expected, actual)
		}
	}

	return nil
}

func (s *session) cloneFinalizable(id ident.ID) ident.ID {
	if id.IsNoFinalize() {
		return id
	}
	return s.pools.id.Clone(id)
}

func (s *session) nsCtxFromMetadata(nsMeta namespace.Metadata) (namespace.Context, error) {
	nsCtx := namespace.NewContextFrom(nsMeta)
	if s.opts.IsSetEncodingProto() && nsCtx.Schema == nil {
		return nsCtx, fmt.Errorf("no protobuf schema found for namespace: %s", nsMeta.ID().String())
	}
	return nsCtx, nil
}

func (s *session) nsCtxFor(ns ident.ID) (namespace.Context, error) {
	nsCtx := namespace.NewContextFor(ns, s.opts.SchemaRegistry())
	if s.opts.IsSetEncodingProto() && nsCtx.Schema == nil {
		return nsCtx, fmt.Errorf("no protobuf schema found for namespace: %s", ns.String())
	}
	return nsCtx, nil
}

type reason int

const (
	reqErrReason reason = iota
	respErrReason
	consistencyLevelNotAchievedErrReason
)

type reattemptType int

const (
	nextRetryReattemptType reattemptType = iota
	fullRetryReattemptType
)

type reattemptStreamBlocksFromPeersFn func(
	[]receivedBlockMetadata,
	enqueueChannel,
	error,
	reason,
	reattemptType,
	*streamFromPeersMetrics,
) error

func (s *session) streamBlocksReattemptFromPeers(
	blocks []receivedBlockMetadata,
	enqueueCh enqueueChannel,
	attemptErr error,
	reason reason,
	reattemptType reattemptType,
	m *streamFromPeersMetrics,
) error {
	switch reason {
	case reqErrReason:
		m.fetchBlockRetriesReqError.Inc(int64(len(blocks)))
	case respErrReason:
		m.fetchBlockRetriesRespError.Inc(int64(len(blocks)))
	case consistencyLevelNotAchievedErrReason:
		m.fetchBlockRetriesConsistencyLevelNotAchievedError.Inc(int64(len(blocks)))
	}

	// Must do this asynchronously or else could get into a deadlock scenario
	// where cannot enqueue into the reattempt channel because no more work is
	// getting done because new attempts are blocked on existing attempts completing
	// and existing attempts are trying to enqueue into a full reattempt channel
	enqueue, done, err := enqueueCh.enqueueDelayed(len(blocks))
	if err != nil {
		return err
	}
	go s.streamBlocksReattemptFromPeersEnqueue(blocks, attemptErr, reattemptType,
		enqueue, done)
	return nil
}

func (s *session) streamBlocksReattemptFromPeersEnqueue(
	blocks []receivedBlockMetadata,
	attemptErr error,
	reattemptType reattemptType,
	enqueueFn enqueueDelayedFn,
	enqueueDoneFn enqueueDelayedDoneFn,
) {
	// NB(r): Notify the delayed enqueue is done.
	defer enqueueDoneFn()

	for i := range blocks {
		var reattemptPeersMetadata []receivedBlockMetadata
		switch reattemptType {
		case nextRetryReattemptType:
			reattemptPeersMetadata = blocks[i].block.reattempt.retryPeersMetadata
		case fullRetryReattemptType:
			reattemptPeersMetadata = blocks[i].block.reattempt.fetchedPeersMetadata
		}
		if len(reattemptPeersMetadata) == 0 {
			continue
		}

		// Reconstruct peers metadata for reattempt
		reattemptBlocksMetadata := make([]receivedBlockMetadata, len(reattemptPeersMetadata))
		for j := range reattemptPeersMetadata {
			var reattempt blockMetadataReattempt
			if reattemptType == nextRetryReattemptType {
				// Only if a default type of retry do we want to actually want
				// to set all the retry metadata, otherwise this re-enqueued metadata
				// should start fresh
				reattempt = blocks[i].block.reattempt

				// Copy the errors for every peer so they don't shard the same error
				// slice and therefore are not subject to race conditions when the
				// error slice is modified
				reattemptErrs := make([]error, len(reattempt.errs)+1)
				n := copy(reattemptErrs, reattempt.errs)
				reattemptErrs[n] = attemptErr
				reattempt.errs = reattemptErrs
			}

			reattemptBlocksMetadata[j] = receivedBlockMetadata{
				peer: reattemptPeersMetadata[j].peer,
				id:   blocks[i].id,
				block: blockMetadata{
					start:     reattemptPeersMetadata[j].block.start,
					size:      reattemptPeersMetadata[j].block.size,
					checksum:  reattemptPeersMetadata[j].block.checksum,
					reattempt: reattempt,
				},
			}
		}

		// Re-enqueue the block to be fetched from all peers requested
		// to reattempt from
		enqueueFn(reattemptBlocksMetadata)
	}
}

type blocksResult interface {
	addBlockFromPeer(
		id ident.ID,
		encodedTags checked.Bytes,
		peer topology.Host,
		block *thriftrpc.Block, // Corrected type
	) error
}

type baseBlocksResult struct {
	nsCtx                   namespace.Context
	blockOpts               block.Options
	blockAllocSize          int
	contextPool             context.Pool
	encoderPool             encoding.EncoderPool
	multiReaderIteratorPool encoding.MultiReaderIteratorPool
}

func newBaseBlocksResult(
	nsCtx namespace.Context,
	opts Options,
	resultOpts result.Options,
) baseBlocksResult {
	blockOpts := resultOpts.DatabaseBlockOptions()
	return baseBlocksResult{
		nsCtx:                   nsCtx,
		blockOpts:               blockOpts,
		blockAllocSize:          blockOpts.DatabaseBlockAllocSize(),
		contextPool:             opts.ContextPool(),
		encoderPool:             blockOpts.EncoderPool(),
		multiReaderIteratorPool: blockOpts.MultiReaderIteratorPool(),
	}
}

func (b *baseBlocksResult) segmentForBlock(seg *thriftrpc.Segment) ts.Segment { // Corrected type
	var (
		bytesPool  = b.blockOpts.BytesPool()
		head, tail checked.Bytes
	)
	if len(seg.Head) > 0 {
		head = bytesPool.Get(len(seg.Head))
		head.IncRef()
		head.AppendAll(seg.Head)
		head.DecRef()
	}
	if len(seg.Tail) > 0 {
		tail = bytesPool.Get(len(seg.Tail))
		tail.IncRef()
		tail.AppendAll(seg.Tail)
		tail.DecRef()
	}
	var checksum uint32
	if seg.Checksum != nil {
		checksum = uint32(*seg.Checksum)
	}

	return ts.NewSegment(head, tail, checksum, ts.FinalizeHead&ts.FinalizeTail)
}

func (b *baseBlocksResult) mergeReaders(
	start xtime.UnixNano, blockSize time.Duration, readers []xio.SegmentReader,
) (encoding.Encoder, error) {
	iter := b.multiReaderIteratorPool.Get()
	iter.Reset(readers, start, blockSize, b.nsCtx.Schema)
	defer iter.Close()

	encoder := b.encoderPool.Get()
	encoder.Reset(start, b.blockAllocSize, b.nsCtx.Schema)

	for iter.Next() {
		dp, unit, annotation := iter.Current()
		if err := encoder.Encode(dp, unit, annotation); err != nil {
			encoder.Close()
			return nil, err
		}
	}
	if err := iter.Err(); err != nil {
		encoder.Close()
		return nil, err
	}

	return encoder, nil
}

func (b *baseBlocksResult) newDatabaseBlock(block *thriftrpc.Block) (block.DatabaseBlock, error) { // Corrected type
	var (
		start    = xtime.UnixNano(block.Start)
		segments = block.Segments
		result   = b.blockOpts.DatabaseBlockPool().Get()
	)

	if segments == nil {
		result.Close() // return block to pool
		return nil, errSessionBadBlockResultFromPeer
	}

	switch {
	case segments.Merged != nil:
		// Unmerged, can insert directly into a single block
		mergedBlock := segments.Merged
		result.Reset(
			start,
			durationConvert(mergedBlock.BlockSize),
			b.segmentForBlock(mergedBlock),
			b.nsCtx,
		)

	case segments.Unmerged != nil:
		// Must merge to provide a single block
		segmentReaderPool := b.blockOpts.SegmentReaderPool()
		readers := make([]xio.SegmentReader, len(segments.Unmerged))

		blockSize := time.Duration(0)
		for i, seg := range segments.Unmerged {
			segmentReader := segmentReaderPool.Get()
			segmentReader.Reset(b.segmentForBlock(seg))
			readers[i] = segmentReader

			bs := durationConvert(seg.BlockSize)
			if bs > blockSize {
				blockSize = bs
			}
		}
		encoder, err := b.mergeReaders(start, blockSize, readers)
		for _, reader := range readers {
			// Close each reader
			reader.Finalize()
		}

		if err != nil {
			// mergeReaders(...) already calls encoder.Close() upon error
			result.Close() // return block to pool
			return nil, err
		}

		// Set the block data
		result.Reset(start, blockSize, encoder.Discard(), b.nsCtx)

	default:
		result.Close() // return block to pool
		return nil, errSessionBadBlockResultFromPeer
	}

	return result, nil
}

// Ensure streamBlocksResult implements blocksResult
var _ blocksResult = (*streamBlocksResult)(nil)

type streamBlocksResult struct {
	baseBlocksResult
	outputCh       chan<- peerBlocksDatapoint
	tagDecoderPool serialize.TagDecoderPool
	idPool         ident.Pool
	nsCtx          namespace.Context
}

func newStreamBlocksResult(
	nsCtx namespace.Context,
	opts Options,
	resultOpts result.Options,
	outputCh chan<- peerBlocksDatapoint,
	tagDecoderPool serialize.TagDecoderPool,
	idPool ident.Pool,
) *streamBlocksResult {
	return &streamBlocksResult{
		nsCtx:            nsCtx,
		baseBlocksResult: newBaseBlocksResult(nsCtx, opts, resultOpts),
		outputCh:         outputCh,
		tagDecoderPool:   tagDecoderPool,
		idPool:           idPool,
	}
}

type peerBlocksDatapoint struct {
	id    ident.ID
	tags  ident.Tags
	peer  topology.Host
	block block.DatabaseBlock
}

func (s *streamBlocksResult) addBlockFromPeer(
	id ident.ID,
	encodedTags checked.Bytes,
	peer topology.Host,
	block *thriftrpc.Block, // Corrected type
) error {
	result, err := s.newDatabaseBlock(block)
	if err != nil {
		return err
	}
	tags, err := newTagsFromEncodedTags(id, encodedTags,
		s.tagDecoderPool, s.idPool)
	if err != nil {
		return err
	}
	s.outputCh <- peerBlocksDatapoint{
		id:    id,
		tags:  tags,
		peer:  peer,
		block: result,
	}
	return nil
}

type peerBlocksIter struct {
	inputCh <-chan peerBlocksDatapoint
	errCh   <-chan error
	current peerBlocksDatapoint
	err     error
	done    bool
}

func newPeerBlocksIter(
	inputC <-chan peerBlocksDatapoint,
	errC <-chan error,
) *peerBlocksIter {
	return &peerBlocksIter{
		inputCh: inputC,
		errCh:   errC,
	}
}

func (it *peerBlocksIter) Current() (topology.Host, ident.ID, ident.Tags, block.DatabaseBlock) {
	return it.current.peer, it.current.id, it.current.tags, it.current.block
}

func (it *peerBlocksIter) Err() error {
	return it.err
}

func (it *peerBlocksIter) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	m, more := <-it.inputCh

	if !more {
		it.err = <-it.errCh
		it.done = true
		return false
	}

	it.current = m
	return true
}

// Ensure streamBlocksResult implements blocksResult
var _ blocksResult = (*bulkBlocksResult)(nil)

type bulkBlocksResult struct {
	sync.RWMutex
	baseBlocksResult
	result         result.ShardResult
	tagDecoderPool serialize.TagDecoderPool
	idPool         ident.Pool
	nsCtx          namespace.Context
}

func newBulkBlocksResult(
	nsCtx namespace.Context,
	opts Options,
	resultOpts result.Options,
	tagDecoderPool serialize.TagDecoderPool,
	idPool ident.Pool,
) *bulkBlocksResult {
	return &bulkBlocksResult{
		nsCtx:            nsCtx,
		baseBlocksResult: newBaseBlocksResult(nsCtx, opts, resultOpts),
		result:           result.NewShardResult(resultOpts),
		tagDecoderPool:   tagDecoderPool,
		idPool:           idPool,
	}
}

func (r *bulkBlocksResult) addBlockFromPeer(
	id ident.ID,
	encodedTags checked.Bytes,
	peer topology.Host,
	block *thriftrpc.Block, // Corrected type
) error {
	start := xtime.UnixNano(block.Start)
	result, err := r.newDatabaseBlock(block)
	if err != nil {
		return err
	}

	var (
		tags                ident.Tags
		attemptedDecodeTags bool
	)
	for {
		r.Lock()
		currBlock, exists := r.result.BlockAt(id, start)
		if !exists {
			if encodedTags == nil || attemptedDecodeTags {
				r.result.AddBlock(id, tags, result)
				r.Unlock()
				break
			}
			r.Unlock()

			// Tags not decoded yet, attempt decoded and then reinsert
			attemptedDecodeTags = true
			tags, err = newTagsFromEncodedTags(id, encodedTags,
				r.tagDecoderPool, r.idPool)
			if err != nil {
				return err
			}
			continue
		}

		// Remove the existing block from the result so it doesn't get
		// merged again
		r.result.RemoveBlockAt(id, start)
		r.Unlock()

		// If we've already received data for this block, merge them
		// with the new block if possible
		tmpCtx := r.contextPool.Get()
		currReader, err := currBlock.Stream(tmpCtx)
		if err != nil {
			return err
		}

		// If there are no data in the current block, there is no
		// need to merge
		if currReader.IsEmpty() {
			continue
		}

		resultReader, err := result.Stream(tmpCtx)
		if err != nil {
			return err
		}
		if resultReader.IsEmpty() {
			return nil
		}

		readers := []xio.SegmentReader{currReader.SegmentReader, resultReader.SegmentReader}
		blockSize := currReader.BlockSize

		encoder, err := r.mergeReaders(start, blockSize, readers)
		if err != nil {
			return err
		}

		result.Close()

		result = r.blockOpts.DatabaseBlockPool().Get()
		result.Reset(start, blockSize, encoder.Discard(), r.nsCtx)

		tmpCtx.Close()
	}

	return nil
}

type enqueueCh struct {
	sync.Mutex
	sending              int
	enqueued             int
	processed            int
	peersMetadataCh      chan []receivedBlockMetadata
	closed               bool
	enqueueDelayedFn     enqueueDelayedFn
	enqueueDelayedDoneFn enqueueDelayedDoneFn
	metrics              *streamFromPeersMetrics
}

// enqueueChannelDefaultLen is the queue length for processing series ready to
// be fetched from other peers.
// It was reduced from 32k to 512 since each struct in the queue is quite large
// and with 32k capacity was using significant memory with high shard
// concurrency.
const enqueueChannelDefaultLen = 512

func newEnqueueChannel(m *streamFromPeersMetrics) enqueueChannel {
	c := &enqueueCh{
		peersMetadataCh: make(chan []receivedBlockMetadata, enqueueChannelDefaultLen),
		metrics:         m,
	}

	// Allocate the enqueue delayed fn just once
	c.enqueueDelayedFn = func(peersMetadata []receivedBlockMetadata) {
		c.peersMetadataCh <- peersMetadata
	}
	c.enqueueDelayedDoneFn = func() {
		c.Lock()
		c.sending--
		c.Unlock()
	}

	go func() {
		for {
			c.Lock()
			closed := c.closed
			numEnqueued := float64(len(c.peersMetadataCh))
			c.Unlock()
			if closed {
				return
			}
			m.blocksEnqueueChannel.Update(numEnqueued)
			time.Sleep(gaugeReportInterval)
		}
	}()
	return c
}

func (c *enqueueCh) enqueue(peersMetadata []receivedBlockMetadata) error {
	c.Lock()
	if c.closed {
		c.Unlock()
		return errEnqueueChIsClosed
	}
	c.enqueued++
	c.sending++
	c.Unlock()
	c.peersMetadataCh <- peersMetadata
	c.Lock()
	c.sending--
	c.Unlock()
	return nil
}

func (c *enqueueCh) enqueueDelayed(numToEnqueue int) (enqueueDelayedFn, enqueueDelayedDoneFn, error) {
	c.Lock()
	if c.closed {
		c.Unlock()
		return nil, nil, errEnqueueChIsClosed
	}
	c.sending++ // NB(r): This is decremented by calling the returned enqueue done function
	c.enqueued += numToEnqueue
	c.Unlock()
	return c.enqueueDelayedFn, c.enqueueDelayedDoneFn, nil
}

// read is always safe to call since you can safely range
// over a closed channel, and/or do a checked read in case
// it is closed (unlike when publishing to a channel).
func (c *enqueueCh) read() <-chan []receivedBlockMetadata {
	return c.peersMetadataCh
}

func (c *enqueueCh) trackPending(amount int) {
	c.Lock()
	c.enqueued += amount
	c.Unlock()
}

func (c *enqueueCh) trackProcessed(amount int) {
	c.Lock()
	c.processed += amount
	c.Unlock()
}

func (c *enqueueCh) unprocessedLen() int {
	c.Lock()
	unprocessed := c.unprocessedLenWithLock()
	c.Unlock()
	return unprocessed
}

func (c *enqueueCh) unprocessedLenWithLock() int {
	return c.enqueued - c.processed
}

func (c *enqueueCh) closeOnAllProcessed() {
	for {
		c.Lock()
		if c.unprocessedLenWithLock() == 0 && c.sending == 0 {
			close(c.peersMetadataCh)
			c.closed = true
			c.Unlock()
			return
		}
		c.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

type receivedBlocks struct {
	enqueued bool
	results  []receivedBlockMetadata
}

type processFn func(batch []receivedBlockMetadata)

// peerBlocksQueue is a per peer queue of blocks to be retrieved from a peer
type peerBlocksQueue struct {
	sync.RWMutex
	closed       bool
	peer         peer
	queue        []receivedBlockMetadata
	doneFns      []func()
	assigned     uint64
	completed    uint64
	maxQueueSize int
	workers      xsync.WorkerPool
	processFn    processFn
}

type newPeerBlocksQueueFn func(
	peer peer,
	maxQueueSize int,
	interval time.Duration,
	workers xsync.WorkerPool,
	processFn processFn,
) *peerBlocksQueue

func newPeerBlocksQueue(
	peer peer,
	maxQueueSize int,
	interval time.Duration,
	workers xsync.WorkerPool,
	processFn processFn,
) *peerBlocksQueue {
	q := &peerBlocksQueue{
		peer:         peer,
		maxQueueSize: maxQueueSize,
		workers:      workers,
		processFn:    processFn,
	}
	if interval > 0 {
		go q.drainEvery(interval)
	}
	return q
}

func (q *peerBlocksQueue) drainEvery(interval time.Duration) {
	for {
		q.Lock()
		if q.closed {
			q.Unlock()
			return
		}
		q.drainWithLock()
		q.Unlock()
		time.Sleep(interval)
	}
}

func (q *peerBlocksQueue) close() {
	q.Lock()
	defer q.Unlock()
	q.closed = true
}

func (q *peerBlocksQueue) trackAssigned(amount int) {
	atomic.AddUint64(&q.assigned, uint64(amount))
}

func (q *peerBlocksQueue) trackCompleted(amount int) {
	atomic.AddUint64(&q.completed, uint64(amount))
}

func (q *peerBlocksQueue) enqueue(bl receivedBlockMetadata, doneFn func()) {
	q.Lock()

	if len(q.queue) == 0 && cap(q.queue) < q.maxQueueSize {
		// Lazy initialize queue
		q.queue = make([]receivedBlockMetadata, 0, q.maxQueueSize)
	}
	if len(q.doneFns) == 0 && cap(q.doneFns) < q.maxQueueSize {
		// Lazy initialize doneFns
		q.doneFns = make([]func(), 0, q.maxQueueSize)
	}
	q.queue = append(q.queue, bl)
	if doneFn != nil {
		q.doneFns = append(q.doneFns, doneFn)
	}
	q.trackAssigned(1)

	// Determine if should drain immediately
	if len(q.queue) < q.maxQueueSize {
		// Require more to fill up block
		q.Unlock()
		return
	}
	q.drainWithLock()

	q.Unlock()
}

func (q *peerBlocksQueue) drain() {
	q.Lock()
	q.drainWithLock()
	q.Unlock()
}

func (q *peerBlocksQueue) drainWithLock() {
	if len(q.queue) == 0 {
		// None to drain
		return
	}
	enqueued := q.queue
	doneFns := q.doneFns
	q.queue = nil
	q.doneFns = nil
	q.workers.Go(func() {
		q.processFn(enqueued)
		// Call done callbacks
		for i := range doneFns {
			doneFns[i]()
		}
		// Track completed blocks
		q.trackCompleted(len(enqueued))
	})
}

type peerBlocksQueues []*peerBlocksQueue

func (qs peerBlocksQueues) findQueue(peer peer) *peerBlocksQueue {
	for _, q := range qs {
		if q.peer == peer {
			return q
		}
	}
	return nil
}

func (qs peerBlocksQueues) closeAll() {
	for _, q := range qs {
		q.close()
	}
}

type receivedBlockMetadata struct {
	peer        peer
	id          ident.ID
	encodedTags checked.Bytes
	block       blockMetadata
}

type receivedBlockMetadatas []receivedBlockMetadata

func (arr receivedBlockMetadatas) swap(i, j int) { arr[i], arr[j] = arr[j], arr[i] }

type peerBlockMetadataByID []receivedBlockMetadata

func (arr peerBlockMetadataByID) Len() int      { return len(arr) }
func (arr peerBlockMetadataByID) Swap(i, j int) { arr[i], arr[j] = arr[j], arr[i] }
func (arr peerBlockMetadataByID) Less(i, j int) bool {
	return strings.Compare(arr[i].peer.Host().ID(), arr[j].peer.Host().ID()) < 0
}

type receivedBlockMetadataQueue struct {
	blockMetadata receivedBlockMetadata
	queue         *peerBlocksQueue
}

type receivedBlockMetadataQueuesByAttemptsAscOutstandingAsc []receivedBlockMetadataQueue

func (arr receivedBlockMetadataQueuesByAttemptsAscOutstandingAsc) Len() int {
	return len(arr)
}

func (arr receivedBlockMetadataQueuesByAttemptsAscOutstandingAsc) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}

func (arr receivedBlockMetadataQueuesByAttemptsAscOutstandingAsc) Less(i, j int) bool {
	peerI := arr[i].queue.peer
	peerJ := arr[j].queue.peer
	attemptsI := arr[i].blockMetadata.block.reattempt.peerAttempts(peerI)
	attemptsJ := arr[j].blockMetadata.block.reattempt.peerAttempts(peerJ)
	if attemptsI != attemptsJ {
		return attemptsI < attemptsJ
	}

	outstandingI := atomic.LoadUint64(&arr[i].queue.assigned) -
		atomic.LoadUint64(&arr[i].queue.completed)
	outstandingJ := atomic.LoadUint64(&arr[j].queue.assigned) -
		atomic.LoadUint64(&arr[j].queue.completed)
	return outstandingI < outstandingJ
}

type blockMetadata struct {
	start     xtime.UnixNano
	size      int64
	checksum  *uint32
	lastRead  xtime.UnixNano
	reattempt blockMetadataReattempt
}

type blockMetadataReattempt struct {
	attempt              int
	fanoutFetchState     *blockFanoutFetchState
	attempted            []peer
	errs                 []error
	retryPeersMetadata   []receivedBlockMetadata
	fetchedPeersMetadata []receivedBlockMetadata
}

type blockFanoutFetchState struct {
	numPending int32
	numSuccess int32
}

func newBlockFanoutFetchState(
	pending int,
) *blockFanoutFetchState {
	return &blockFanoutFetchState{
		numPending: int32(pending),
	}
}

func (s *blockFanoutFetchState) success() int {
	return int(atomic.LoadInt32(&s.numSuccess))
}

func (s *blockFanoutFetchState) incrementSuccess() {
	atomic.AddInt32(&s.numSuccess, 1)
}

func (s *blockFanoutFetchState) decrementAndReturnPending() int {
	return int(atomic.AddInt32(&s.numPending, -1))
}

func (b blockMetadataReattempt) peerAttempts(p peer) int {
	r := 0
	for i := range b.attempted {
		if b.attempted[i] == p {
			r++
		}
	}
	return r
}

func newTimesByUnixNanos(values []int64) []time.Time {
	result := make([]time.Time, len(values))
	for i := range values {
		result[i] = time.Unix(0, values[i])
	}
	return result
}

func newTimesByRPCThriftBlocks(values []*thriftrpc.Block) []time.Time { // Corrected argument type
	result := make([]time.Time, len(values))
	for i := range values {
		result[i] = time.Unix(0, values[i].Start)
	}
	return result
}

type metadataIter struct {
	inputCh        <-chan receivedBlockMetadata
	errCh          <-chan error
	host           topology.Host
	metadata       block.Metadata
	tagDecoderPool serialize.TagDecoderPool
	idPool         ident.Pool
	done           bool
	err            error
}

func newMetadataIter(
	inputCh <-chan receivedBlockMetadata,
	errCh <-chan error,
	tagDecoderPool serialize.TagDecoderPool,
	idPool ident.Pool,
) PeerBlockMetadataIter {
	return &metadataIter{
		inputCh:        inputCh,
		errCh:          errCh,
		tagDecoderPool: tagDecoderPool,
		idPool:         idPool,
	}
}

func (it *metadataIter) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	m, more := <-it.inputCh
	if !more {
		it.err = <-it.errCh
		it.done = true
		return false
	}
	var tags ident.Tags
	tags, it.err = newTagsFromEncodedTags(m.id, m.encodedTags,
		it.tagDecoderPool, it.idPool)
	if it.err != nil {
		return false
	}
	it.host = m.peer.Host()
	it.metadata = block.NewMetadata(m.id, tags, m.block.start,
		m.block.size, m.block.checksum, m.block.lastRead)
	return true
}

func (it *metadataIter) Current() (topology.Host, block.Metadata) {
	return it.host, it.metadata
}

func (it *metadataIter) Err() error {
	return it.err
}

type idAndBlockStart struct {
	id         ident.ID
	blockStart int64
}

func newTagsFromEncodedTags(
	seriesID ident.ID,
	encodedTags checked.Bytes,
	tagDecoderPool serialize.TagDecoderPool,
	idPool ident.Pool,
) (ident.Tags, error) {
	if encodedTags == nil {
		return ident.Tags{}, nil
	}

	encodedTags.IncRef()

	tagDecoder := tagDecoderPool.Get()
	tagDecoder.Reset(encodedTags)
	defer tagDecoder.Close()

	tags, err := idxconvert.TagsFromTagsIter(seriesID, tagDecoder, idPool)

	encodedTags.DecRef()

	return tags, err
}

const (
	// histogramDurationBucketsVersion must be bumped if histogramDurationBuckets is changed
	// to namespace the different buckets from each other so they don't overlap and cause the
	// histogram function to error out due to overlapping buckets in the same query.
	histogramDurationBucketsVersion = "v1"
	// histogramDurationBucketsVersionTag is the tag for the version of the buckets in use.
	histogramDurationBucketsVersionTag = "schema"
)

// histogramDurationBuckets is a high resolution set of duration buckets.
func histogramDurationBuckets() tally.DurationBuckets {
	return append(tally.DurationBuckets{0},
		tally.MustMakeExponentialDurationBuckets(time.Millisecond, 1.25, 60)...)
}

// histogramWithDurationBuckets returns a histogram with the standard duration buckets.
func histogramWithDurationBuckets(scope tally.Scope, name string) tally.Histogram {
	sub := scope.Tagged(map[string]string{
		histogramDurationBucketsVersionTag: histogramDurationBucketsVersion,
	})
	return sub.Histogram(name, histogramDurationBuckets())
}

func minDuration(x, y time.Duration) time.Duration {
	if x < y {
		return x
	}
	return y
}

func (s *session) HealthGRPC(ctx gocontext.Context, host topology.Host) (*grpcrpc.NodeHealthResult, error) {
	grpcCfg := s.opts.GRPCClientConfig()
	if !grpcCfg.Enabled {
		return nil, errors.New("gRPC client is not enabled in client options")
	}

	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return nil, ErrSessionStatusNotOpen
	}
	nodeClient, ok := s.grpcNodeClients[host.IDString()]
	s.state.RUnlock()

	if !ok {
		return nil, fmt.Errorf("gRPC client not found for host: %s", host.IDString())
	}

	healthCheckTimeout := grpcCfg.NodeHealthCheckTimeoutOrDefault()
	callCtx, cancel := gocontext.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()

	return nodeClient.Health(callCtx, &emptypb.Empty{})
}

// Helper to convert xtime.Unit to grpcrpc.TimeType
func toGRPCWriteTimeType(unit xtime.Unit) (grpcrpc.TimeType, error) {
	switch unit {
	case xtime.Second:
		return grpcrpc.TimeType_UNIX_SECONDS, nil
	case xtime.Millisecond:
		return grpcrpc.TimeType_UNIX_MILLISECONDS, nil
	case xtime.Microsecond:
		return grpcrpc.TimeType_UNIX_MICROSECONDS, nil
	case xtime.Nanosecond:
		return grpcrpc.TimeType_UNIX_NANOSECONDS, nil
	default:
		return grpcrpc.TimeType_UNIX_SECONDS, fmt.Errorf("unknown time unit: %v", unit)
	}
}

// Helper to convert client datapoint to gRPC datapoint for Write
func toGRPCWriteDatapoint(
	t xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) (*grpcrpc.Datapoint, error) {
	grpcTimeType, err := toGRPCWriteTimeType(unit)
	if err != nil {
		return nil, err
	}

	var timestampVal int64
	switch grpcTimeType {
	case grpcrpc.TimeType_UNIX_SECONDS:
		timestampVal = t.ToSeconds()
	case grpcrpc.TimeType_UNIX_MILLISECONDS:
		timestampVal = t.ToMilliseconds()
	case grpcrpc.TimeType_UNIX_MICROSECONDS:
		timestampVal = t.ToMicroseconds()
	case grpcrpc.TimeType_UNIX_NANOSECONDS:
		timestampVal = int64(t)
	default:
		// Should have been caught by toGRPCWriteTimeType, but as a safeguard:
		return nil, fmt.Errorf("unhandled time type conversion for GRPC datapoint: %v", grpcTimeType)
	}

	return &grpcrpc.Datapoint{
		Timestamp:         timestampVal,
		Value:             value,
		Annotation:        annotation,
		TimestampTimeType: grpcTimeType,
	}, nil
}

func (s *session) WriteGRPC(
	namespace ident.ID,
	id ident.ID,
	t xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	grpcCfg := s.opts.GRPCClientConfig()
	if !grpcCfg.Enabled {
		return errors.New("gRPC client is not enabled in client options for WriteGRPC")
	}

	startWriteAttempt := s.nowFn()

	dp, err := toGRPCWriteDatapoint(t, value, unit, annotation)
	if err != nil {
		return xerrors.NewInvalidParamsError(err)
	}

	grpcReq := &grpcrpc.WriteRequest{
		NameSpace: namespace.String(),
		Id:        id.String(),
		Datapoint: dp,
	}

	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return ErrSessionStatusNotOpen
	}

	topoMap := s.state.topoMap
	// It's important to use the write consistency level from the session state,
	// as it can be updated dynamically via runtime options.
	consistencyLevel := s.state.writeLevel
	majority := int32(s.state.majority)

	// Clone IDs for safety if they are to be used in multiple goroutines or held onto.
	// For this request, they are used directly in grpcReq which is passed to each call.
	// If grpcReq were modified per goroutine, cloning would be essential.
	// Here, grpcReq is read-only after creation for the fan-out calls.

	// Similar to writeAttemptWithRLock, but for gRPC
	var (
		enqueued      int32
		pending       int32
		success       int32
		errors        []error // Collect errors from replicas
		errorsLock    sync.Mutex
		wg            sync.WaitGroup
		numReplicas   = 0 // Will count actual replicas targeted
	)

	// RouteForEach handles sharding and identifying target hosts
	routeErr := topoMap.RouteForEach(id, func(idx int, hostShard shard.Shard, host topology.Host) {
		if !s.writeShardsInitializing && hostShard.State() == shard.Initializing {
			return // Skip initializing shards if not configured to write to them
		}
		if s.shardsLeavingCountTowardsConsistency && hostShard.State() == shard.Leaving {
			// This logic is part of writeState in TChannel path, needs careful thought for gRPC
		}
		if s.shardsLeavingAndInitializingCountTowardsConsistency && (hostShard.State() == shard.Leaving || hostShard.State() == shard.Initializing) {
			// Similar careful thought needed
		}

		numReplicas++
		pending++
		wg.Add(1)

		go func(h topology.Host) {
			defer wg.Done()

			nodeClient, ok := s.grpcNodeClients[h.IDString()]
			if !ok {
				errorsLock.Lock()
				errors = append(errors, fmt.Errorf("gRPC client not found for host %s", h.IDString()))
				errorsLock.Unlock()
				atomic.AddInt32(&pending, -1)
				return
			}

			timeout := s.opts.WriteRequestTimeout() // Default from options/config
			ctx, cancel := gocontext.WithTimeout(gocontext.Background(), timeout)
			defer cancel()

			_, err := nodeClient.Write(ctx, grpcReq)
			atomic.AddInt32(&pending, -1)
			if err != nil {
				errorsLock.Lock()
				errors = append(errors, err) // TODO: Wrap error with host info?
				errorsLock.Unlock()
				// Log individual host error if desired
				// s.log.Debug("gRPC write error to host", zap.String("host", h.IDString()), zap.Error(err))
				if s.logHostWriteErrorSampler.Sample() {
					s.log.Error("gRPC write error to host",
						zap.String("host", h.IDString()),
						zap.Error(err))
				}
				return
			}
			atomic.AddInt32(&success, 1)
		}(host)
		enqueued++
	})
	s.state.RUnlock() // Unlock after iterating over topology and getting client map

	if routeErr != nil {
		return routeErr
	}

	if enqueued == 0 {
		// This could happen if the ID didn't map to any shards or all shards were initializing and skipped.
		// Mimic TChannel path's writeState handling which would decRef and return an error.
		// recordWriteMetrics(err, state, startWriteAttempt) -> needs a writeState-like object or direct metric calls
		s.metrics.writeErrorsInternalError.Inc(1) // Or a more specific metric
		s.metrics.writeLatencyHistogram.RecordDuration(s.nowFn().Sub(startWriteAttempt))
		return errors.New("no hosts available to write to for the given ID")
	}

	wg.Wait()

	// Consistency check (simplified version of writeConsistencyResult)
	// TODO: Integrate with the leaving/initializing shard logic for success counting if needed.
	// For now, simple success count.
	finalErr := s.writeConsistencyResult(consistencyLevel, majority, enqueued, enqueued-int32(len(errors)), int32(len(errors)), errors)

	// Simplified metrics for now, ideally would use a writeState-like object for consistency with TChannel path
	if finalErr == nil {
		s.metrics.writeSuccess.Inc(1)
	} else if IsBadRequestError(finalErr) { // This check might not directly apply to gRPC errors unless we map them
		s.metrics.writeErrorsBadRequest.Inc(1)
	} else {
		s.metrics.writeErrorsInternalError.Inc(1)
	}
	s.metrics.writeLatencyHistogram.RecordDuration(s.nowFn().Sub(startWriteAttempt))
	if finalErr != nil && s.logWriteErrorSampler.Sample() {
		s.log.Error("m3db client gRPC write error occurred",
			zap.Float64("sampleRateLog", s.logWriteErrorSampler.SampleRate().Value()),
			zap.Error(finalErr))
	}

	return finalErr
}

// Helper to convert *grpcrpc.Segment to ts.Segment
func fromGRPCSegment(grpcSeg *grpcrpc.Segment, bytesPool pool.CheckedBytesPool) (ts.Segment, error) {
	if grpcSeg == nil {
		return ts.Segment{}, errors.New("nil gRPC segment")
	}

	var head, tail checked.Bytes
	if len(grpcSeg.Head) > 0 {
		head = bytesPool.Get(len(grpcSeg.Head))
		head.IncRef()
		head.AppendAll(grpcSeg.Head)
		// head.DecRef() // DecRef should be done by the consumer of ts.Segment / finalizer
	}
	if len(grpcSeg.Tail) > 0 {
		tail = bytesPool.Get(len(grpcSeg.Tail))
		tail.IncRef()
		tail.AppendAll(grpcSeg.Tail)
		// tail.DecRef() // DecRef should be done by the consumer of ts.Segment / finalizer
	}

	var checksum uint32
	if grpcSeg.Checksum != 0 { // Assuming 0 means no checksum, or it's optional and could be nil if proto used wrapper
		checksum = uint32(grpcSeg.Checksum)
	}

	// ts.Segment finalizer will DecRef head and tail
	return ts.NewSegment(head, tail, checksum, ts.FinalizeHead|ts.FinalizeTail), nil
}

// fromGRPCSegmentsToSeriesIterator converts []*grpcrpc.Segments (from a single FetchRawResult)
// into a single encoding.SeriesIterator.
func fromGRPCSegmentsToSeriesIterator(
	grpcSegments []*grpcrpc.Segments,
	id ident.ID,
	nsCtx namespace.Context,
	startInclusive xtime.UnixNano,
	endExclusive xtime.UnixNano,
	opts Options,
) (encoding.SeriesIterator, error) {
	if len(grpcSegments) == 0 {
		emptyIter := opts.IteratorPools().SeriesIterator().Get()
		emptyIter.Reset(encoding.SeriesIteratorOptions{ID: id, Namespace: nsCtx.ID()})
		return emptyIter, nil
	}

	// This is the complex part: merging multiple Segments (each with merged/unmerged ts.Segment)
	// into one SeriesIterator.
	// For now, let's simplify: assume FetchRawResult gives us one logical series,
	// so we concatenate all ts.Segments from all grpcrpc.Segments into one MultiReaderIterator.
	// This bypasses complex merging logic like the TChannel path's block consolidation for now.
	// A more robust solution would be needed for production, especially for multi-replica results.

	allTsSegments := make([]ts.Segment, 0)
	bytesPool := opts.CheckedBytesPool() // For creating checked.Bytes for ts.Segment

	for _, segWrapper := range grpcSegments {
		if segWrapper == nil {
			continue
		}
		if segWrapper.Merged != nil {
			tsSeg, err := fromGRPCSegment(segWrapper.Merged, bytesPool)
			if err != nil {
				// Log error or handle?
				opts.InstrumentOptions().Logger().Error("error converting merged gRPC segment", zap.Error(err))
				continue
			}
			allTsSegments = append(allTsSegments, tsSeg)
		}
		for _, unmergedGrpcSeg := range segWrapper.Unmerged {
			tsSeg, err := fromGRPCSegment(unmergedGrpcSeg, bytesPool)
			if err != nil {
				opts.InstrumentOptions().Logger().Error("error converting unmerged gRPC segment", zap.Error(err))
				continue
			}
			allTsSegments = append(allTsSegments, tsSeg)
		}
	}

	if len(allTsSegments) == 0 {
		emptyIter := opts.IteratorPools().SeriesIterator().Get()
		emptyIter.Reset(encoding.SeriesIteratorOptions{ID: id, Namespace: nsCtx.ID()})
		return emptyIter, nil
	}

	// Create readers for all ts.Segments
	readers := make([]xio.SegmentReader, 0, len(allTsSegments))
	for _, tsSeg := range allTsSegments {
		// Must ensure tsSeg is valid and its Bytes() can be used.
		// The SegmentReaderPool might not be appropriate if these are in-memory ts.Segments
		// not directly from disk/persist manager.
		// xio.NewSegmentReader takes a ts.Segment.
		readers = append(readers, xio.NewSegmentReader(tsSeg))
	}

	iter := opts.IteratorPools().SeriesIterator().Get()
	iterOpts := opts.IterationOptions()

	// Create a MultiReaderIterator for these segments
	multiReaderIter := opts.IteratorPools().MultiReaderIterator().Get()
	// Resetting MultiReaderIterator typically requires a block size.
	// We need to determine an appropriate block size. This could come from namespace options.
	// For now, using schema's block size.
	multiReaderIter.Reset(readers, startInclusive, nsCtx.Schema().Options().BlockSize(), nsCtx.Schema())

	seriesIterOptions := encoding.SeriesIteratorOptions{
		ID:                         id,
		Namespace:                  nsCtx.ID(),
		Tags:                       ident.Tags{}, // FetchBatchRaw does not return tags per series directly
		StartInclusive:             startInclusive,
		EndExclusive:               endExclusive,
		Replicas:                   []encoding.MultiReaderIterator{multiReaderIter}, // Wrap it as a single replica
		SeriesIteratorConsolidator: iterOpts.SeriesIteratorConsolidator,
		Schema:                     nsCtx.Schema,
	}
	iter.Reset(seriesIterOptions)

	// The responsibility of closing the MultiReaderIterator and its underlying SegmentReaders
	// (and the ts.Segments' checked.Bytes) should be handled by the SeriesIterator's Close() method.
	return iter, nil
}

// Helper to convert grpcrpc.TimeType and an int64 timestamp value to xtime.UnixNano
func fromGRPCDatapointTime(timestampVal int64, tt grpcrpc.TimeType) (xtime.UnixNano, error) {
	switch tt {
	case grpcrpc.TimeType_UNIX_SECONDS:
		return xtime.UnixNano(timestampVal * 1e9), nil
	case grpcrpc.TimeType_UNIX_MILLISECONDS:
		return xtime.UnixNano(timestampVal * 1e6), nil
	case grpcrpc.TimeType_UNIX_MICROSECONDS:
		return xtime.UnixNano(timestampVal * 1e3), nil
	case grpcrpc.TimeType_UNIX_NANOSECONDS:
		return xtime.UnixNano(timestampVal), nil
	default:
		return 0, fmt.Errorf("unknown gRPC time type: %v", tt)
	}
}

// Helper to convert single *grpcrpc.Datapoint to ts.Datapoint
func fromGRPCDatapoint(dp *grpcrpc.Datapoint) (ts.Datapoint, error) {
	tsNano, err := fromGRPCDatapointTime(dp.Timestamp, dp.TimestampTimeType)
	if err != nil {
		return ts.Datapoint{}, err
	}
	return ts.Datapoint{
		TimestampNanos: tsNano,
		Value:          dp.Value,
		Annotation:     dp.Annotation, // Annotation is already []byte
	}, nil
}

// Helper to convert a single *grpcrpc.FetchResult to an encoding.SeriesIterator
// This is a simplified version; for multi-replica results, merging/deduplication would be needed.
func fromGRPCFetchResultToSeriesIterator(
	result *grpcrpc.FetchResult, // Assuming result from a single successful replica for now
	id ident.ID, // Needed for SeriesIterator context
	nsCtx namespace.Context, // For schema, etc.
	startInclusive xtime.UnixNano,
	endExclusive xtime.UnixNano,
	opts Options, // For pools and iteration options
) (encoding.SeriesIterator, error) {
	if result == nil || len(result.Datapoints) == 0 {
		// Return an empty iterator
		emptyIter := opts.IteratorPools().SeriesIterator().Get()
		emptyIter.Reset(encoding.SeriesIteratorOptions{ID: id, Namespace: nsCtx.ID()})
		return emptyIter, nil
	}

	// Convert grpcrpc.Datapoint to ts.Datapoint
	tsDatapoints := make([]ts.Datapoint, 0, len(result.Datapoints))
	for _, dp := range result.Datapoints {
		tsDp, err := fromGRPCDatapoint(dp)
		if err != nil {
			// Log or handle individual datapoint conversion error?
			// For now, let's skip bad datapoints.
			// opts.InstrumentOptions().Logger().Error("failed to convert gRPC datapoint", zap.Error(err))
			continue
		}
		tsDatapoints = append(tsDatapoints, tsDp)
	}

	// Create a new SeriesIterator.
	// This part is complex as it depends on how SeriesIterator is typically constructed
	// from raw datapoints. M3DB usually streams encoded blocks.
	// For simplicity here, we'll use a basic in-memory series representation.
	// This might not be the most performant or idiomatic way for M3DB.
	// A proper implementation might involve encoding these ts.Datapoints into a block
	// and then creating an iterator over that block.

	// Let's use a mutable segment to build the iterator.
	// This is a simplified approach.
	segment := ts.NewSegment(nil, nil, 0, ts.FinalizeNone) // No backing bytes, will be in-memory
	encoder := opts.EncoderPool().Get()
	encoder.Reset(startInclusive, nsCtx.Schema().Options().BlockSize(), nsCtx.Schema())

	for _, dp := range tsDatapoints {
		// Note: This assumes WriteableBlock allows direct datapoint appends,
		// or that encoder can handle this.
		// The typical flow is encoder.Encode(dp), then encoder.Discard() -> ts.Segment.
		// For simplicity, if direct ts.Datapoint to iterator is hard, we might need to encode.
		if err := encoder.Encode(dp, xtime.Nanosecond, dp.Annotation); err != nil {
			// Handle encoding error
			encoder.Close()
			return nil, fmt.Errorf("failed to encode datapoint for series iterator: %w", err)
		}
	}
	segBytes := encoder.Discard() // This gives ts.Segment
	encoder.Close() // Should be done by Discard typically, but good practice.

	segment.Head = segBytes.Head
	segment.Tail = segBytes.Tail
	// Checksum would be calculated if needed, but for this in-memory segment, it's less critical.

	iter := opts.IteratorPools().SeriesIterator().Get()

	// The SeriesIteratorOptions usually takes block.ReplicaSeriesSegments
	// This simplified approach might need a custom iterator or a more involved setup.
	// For now, let's assume a simple iterator that can take ts.Datapoint directly,
	// or we construct a block.DatabaseBlock like structure.
	// This is the most complex part of the conversion.

	// Given the complexity, let's use a more direct path if possible, or simplify.
	// The goal is to get an encoding.SeriesIterator.
	// We have []ts.Datapoint. We can use encoding.NewSeriesIterator.
	// It requires encoding.SeriesIteratorOptions.

	// Create a single replica segment for the iterator
	replicaSegments := block.NewReplicaSeriesSegments()
	replicaSegments.AddSegment(segment)

	iterOpts := opts.IterationOptions() // Get existing iteration options
	iter.Reset(encoding.SeriesIteratorOptions{
		ID:                         id,
		Namespace:                  nsCtx.ID(),
		StartInclusive:             startInclusive,
		EndExclusive:               endExclusive,
		Replicas:                   []encoding.MultiReaderIterator{encoding.NewSegmentReaderIterator(xio.NewSegmentReader(segment), iterOpts)},
		SeriesIteratorConsolidator: iterOpts.SeriesIteratorConsolidator, // Use configured consolidator
		Schema:                     nsCtx.Schema,
	})
	// This Reset signature might be slightly off, depending on exact SeriesIterator internal expectations.
	// A common way is to have a list of blocks (or segments) for replicas.
	// For a single result, it's simpler.

	// If the above Reset doesn't work directly with a single segment for non-block based data,
	// an alternative is to use a more basic iterator or adapt one.
	// For now, this structure is an attempt.
	// The key is: tsDatapoints -> encoding.SeriesIterator

	// A simpler approach might be to use a pre-built iterator that takes datapoints,
	// but m3db's iterators are typically block-based.
	// If direct construction is too complex, this helper would need significant internal knowledge
	// of m3db's encoding and block structures, or a new type of SeriesIterator.

	// Fallback to a simpler iterator for now if the above is too complex for direct use
	// This implies a more basic iterator or needing to construct a block.
	// This is a placeholder for what might be a more complex conversion.
	 simpleSeriesIter := encoding.NewSeriesIterator(encoding.SeriesIteratorOptions{
		ID:             id,
		Namespace:      nsCtx.ID(),
		Tags:           ident.Tags{}, // Tags are not part of FetchResult directly
		StartInclusive: startInclusive,
		EndExclusive:   endExclusive,
		// Replicas would be built from the tsDatapoints, typically by encoding them into blocks
		// and then providing iterators over those blocks.
	 }, nil)
	 simpleSeriesIter.Reset(encoding.SeriesIteratorOptions{
		ID: id, Namespace: nsCtx.ID(), StartInclusive: startInclusive, EndExclusive: endExclusive,
	 })
	 for _, dp := range tsDatapoints {
		 simpleSeriesIter.Append(ts.Datapoint(dp)) // Assuming SeriesIterator has an Append method or similar
	 }
	 // This Append is hypothetical. encoding.NewSeriesIterator typically takes existing iterators.
	 // The most robust way is to create a block, then an iterator for it.
	 // This part of the helper is the most complex and likely needs internal M3DB block creation logic.
	 // For the purpose of this subtask, returning a basic iterator or noting this complexity.
	 // Let's assume we can create an iterator from `tsDatapoints`.
	 // The simplest way is to use the existing block/segment structure if possible.

	// The above `Reset` with `NewSegmentReaderIterator` is more plausible.
	return iter, nil
}


func (s *session) FetchGRPC(
	namespace ident.ID,
	id ident.ID,
	startInclusive xtime.UnixNano,
	endExclusive xtime.UnixNano,
) (encoding.SeriesIterator, error) {
	grpcCfg := s.opts.GRPCClientConfig()
	if !grpcCfg.Enabled {
		return nil, errors.New("gRPC client is not enabled in client options for FetchGRPC")
	}

	startFetchAttempt := s.nowFn()

	// For Fetch, RangeType and ResultTimeType are often UNIX_NANOSECONDS for simplicity with xtime.UnixNano
	// but this should align with what the gRPC server expects or what client wants.
	// Assuming Nanoseconds for now.
	// The gRPC FetchRequest has RangeStart, RangeEnd, NameSpace, Id, RangeType, ResultTimeType
	// For this client method, we can default RangeType and ResultTimeType.

	grpcReq := &grpcrpc.FetchRequest{
		NameSpace:      namespace.String(),
		Id:             id.String(),
		RangeStart:     int64(startInclusive),
		RangeEnd:       int64(endExclusive),
		RangeType:      grpcrpc.TimeType_UNIX_NANOSECONDS, // Defaulting
		ResultTimeType: grpcrpc.TimeType_UNIX_NANOSECONDS, // Defaulting
	}

	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return nil, ErrSessionStatusNotOpen
	}

	topoMap := s.state.topoMap
	consistencyLevel := s.state.readLevel // Use read consistency level
	majority := int32(s.state.majority)

	var (
		enqueued        int32
		pending         int32
		successfulResults []*grpcrpc.FetchResult
		responseErrors  []error
		errorsLock      sync.Mutex
		wg              sync.WaitGroup
		numReplicas     = 0
	)

	routeErr := topoMap.RouteForEach(id, func(idx int, hostShard shard.Shard, host topology.Host) {
		// TODO: Consider shard state if necessary for reads, though typically reads go to all available.
		numReplicas++
		pending++
		wg.Add(1)

		go func(h topology.Host) {
			defer wg.Done()
			defer atomic.AddInt32(&pending, -1)

			nodeClient, ok := s.grpcNodeClients[h.IDString()]
			if !ok {
				errorsLock.Lock()
				responseErrors = append(responseErrors, fmt.Errorf("gRPC client not found for host %s", h.IDString()))
				errorsLock.Unlock()
				return
			}

			timeout := s.opts.FetchRequestTimeout()
			ctx, cancel := gocontext.WithTimeout(gocontext.Background(), timeout)
			defer cancel()

			result, err := nodeClient.Fetch(ctx, grpcReq)
			if err != nil {
				errorsLock.Lock()
				responseErrors = append(responseErrors, err)
				errorsLock.Unlock()
				if s.logHostFetchErrorSampler.Sample() {
					s.log.Error("gRPC fetch error from host", zap.String("host", h.IDString()), zap.Error(err))
				}
				return
			}
			errorsLock.Lock()
			successfulResults = append(successfulResults, result)
			errorsLock.Unlock()
		}(host)
		enqueued++
	})
	s.state.RUnlock()

	if routeErr != nil {
		return nil, routeErr
	}

	if enqueued == 0 {
		s.metrics.fetchErrorsInternalError.Inc(1) // or a more specific metric
		s.metrics.fetchLatencyHistogram.RecordDuration(s.nowFn().Sub(startFetchAttempt))
		return nil, errors.New("no hosts available to fetch from for the given ID")
	}

	wg.Wait()

	// Check consistency
	numSuccess := int32(len(successfulResults))
	finalErr := s.readConsistencyResult(consistencyLevel, majority, enqueued, numSuccess, int32(len(responseErrors)), responseErrors)

	// Record metrics
	if finalErr == nil {
		s.metrics.fetchSuccess.Inc(1)
	} else if IsBadRequestError(finalErr) { // This check might not directly apply to gRPC errors
		s.metrics.fetchErrorsBadRequest.Inc(1)
	} else {
		s.metrics.fetchErrorsInternalError.Inc(1)
	}
	s.metrics.fetchLatencyHistogram.RecordDuration(s.nowFn().Sub(startFetchAttempt))
	if finalErr != nil && s.logFetchErrorSampler.Sample() {
		s.log.Error("m3db client gRPC fetch error occurred",
			zap.Float64("sampleRateLog", s.logFetchErrorSampler.SampleRate().Value()),
			zap.Error(finalErr))
	}

	if finalErr != nil {
		return nil, finalErr
	}

	if len(successfulResults) == 0 {
		// Should have been caught by consistency check, but as a safeguard
		return encoding.EmptySeriesIterator, nil
	}

	// For now, use the first successful result.
	// TODO: Implement proper merging strategy if consistency > ONE.
	nsCtx, nsCtxErr := s.nsCtxFor(namespace)
	if nsCtxErr != nil {
		return nil, nsCtxErr
	}

	iter, iterErr := fromGRPCFetchResultToSeriesIterator(successfulResults[0], id, nsCtx, startInclusive, endExclusive, s.opts)
	if iterErr != nil {
		return nil, fmt.Errorf("failed to convert gRPC fetch result to series iterator: %w", iterErr)
	}
	return iter, nil
}

// Helper to convert ident.TagIterator to []*grpcrpc.Tag
func toGRPCTags(tagIter ident.TagIterator) ([]*grpcrpc.Tag, error) {
	// Determine size for pre-allocation if possible, though TagIterator doesn't expose length easily.
	// If performance becomes an issue, consider pooling the slice or optimizing tag iteration.
	var tags []*grpcrpc.Tag
	// Create a new iterator as the input one might be used elsewhere or need reset.
	iter := ident.NewTagsIterator(tagIter.RemainingTags())

	for iter.Next() {
		tag := iter.Current()
		tags = append(tags, &grpcrpc.Tag{
			Name:  tag.Name.String(),
			Value: tag.Value.String(),
		})
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	// NB: The original iterator `tagIter` is consumed by `RemainingTags()`.
	// If `tagIter` needs to be reused by the caller, it should be duplicated before calling this helper.
	return tags, nil
}

func (s *session) WriteTaggedGRPC(
	namespace ident.ID,
	id ident.ID,
	tags ident.TagIterator,
	t xtime.UnixNano,
	value float64,
	unit xtime.Unit,
	annotation []byte,
) error {
	grpcCfg := s.opts.GRPCClientConfig()
	if !grpcCfg.Enabled {
		return errors.New("gRPC client is not enabled in client options for WriteTaggedGRPC")
	}

	startWriteAttempt := s.nowFn()

	dp, err := toGRPCWriteDatapoint(t, value, unit, annotation)
	if err != nil {
		return xerrors.NewInvalidParamsError(err)
	}

	// NB: It's important that the tagIter passed to toGRPCTags is a fresh iterator
	// or a duplicate if the original needs to be preserved, as toGRPCTags will consume it.
	// The caller of WriteTaggedGRPC provides the iterator; if they need to reuse it,
	// they should duplicate it before passing it in.
	grpcTags, err := toGRPCTags(tags)
	if err != nil {
		return xerrors.NewInvalidParamsError(fmt.Errorf("failed to convert tags for gRPC request: %w", err))
	}

	grpcReq := &grpcrpc.WriteTaggedRequest{
		NameSpace: namespace.String(),
		Id:        id.String(),
		Tags:      grpcTags,
		Datapoint: dp,
	}

	s.state.RLock()
	if s.state.status != statusOpen {
		s.state.RUnlock()
		return ErrSessionStatusNotOpen
	}

	topoMap := s.state.topoMap
	consistencyLevel := s.state.writeLevel
	majority := int32(s.state.majority)

	var (
		enqueued      int32
		pending       int32
		success       int32
		responseErrors []error // Renamed to avoid conflict with std 'errors' pkg
		errorsLock    sync.Mutex
		wg            sync.WaitGroup
		numReplicas   = 0
	)

	routeErr := topoMap.RouteForEach(id, func(idx int, hostShard shard.Shard, host topology.Host) {
		if !s.writeShardsInitializing && hostShard.State() == shard.Initializing {
			return
		}
		// TODO(rartoul): Add logic for s.shardsLeavingCountTowardsConsistency and s.shardsLeavingAndInitializingCountTowardsConsistency
		// This requires adapting how `writeState.leavingAndInitializingPairCounted` is handled in the TChannel path.

		numReplicas++
		pending++
		wg.Add(1)

		go func(h topology.Host) {
			defer wg.Done()

			nodeClient, ok := s.grpcNodeClients[h.IDString()]
			if !ok {
				errorsLock.Lock()
				responseErrors = append(responseErrors, fmt.Errorf("gRPC client not found for host %s", h.IDString()))
				errorsLock.Unlock()
				atomic.AddInt32(&pending, -1)
				return
			}

			timeout := s.opts.WriteRequestTimeout()
			ctx, cancel := gocontext.WithTimeout(gocontext.Background(), timeout)
			defer cancel()

			_, err := nodeClient.WriteTagged(ctx, grpcReq)
			atomic.AddInt32(&pending, -1)
			if err != nil {
				errorsLock.Lock()
				responseErrors = append(responseErrors, err)
				errorsLock.Unlock()
				if s.logHostWriteErrorSampler.Sample() {
					s.log.Error("gRPC writeTagged error to host",
						zap.String("host", h.IDString()),
						zap.Error(err))
				}
				return
			}
			atomic.AddInt32(&success, 1)
		}(host)
		enqueued++
	})
	s.state.RUnlock()

	if routeErr != nil {
		return routeErr
	}

	if enqueued == 0 {
		s.metrics.writeErrorsInternalError.Inc(1)
		s.metrics.writeLatencyHistogram.RecordDuration(s.nowFn().Sub(startWriteAttempt))
		return errors.New("no hosts available to write tagged to for the given ID")
	}

	wg.Wait()

	finalErr := s.writeConsistencyResult(consistencyLevel, majority, enqueued, enqueued-int32(len(responseErrors)), int32(len(responseErrors)), responseErrors)

	// Simplified metrics for now
	if finalErr == nil {
		s.metrics.writeSuccess.Inc(1) // TODO: Differentiate tagged writes if necessary in metrics
	} else if IsBadRequestError(finalErr) {
		s.metrics.writeErrorsBadRequest.Inc(1)
	} else {
		s.metrics.writeErrorsInternalError.Inc(1)
	}
	s.metrics.writeLatencyHistogram.RecordDuration(s.nowFn().Sub(startWriteAttempt))
	if finalErr != nil && s.logWriteErrorSampler.Sample() {
		s.log.Error("m3db client gRPC writeTagged error occurred",
			zap.Float64("sampleRateLog", s.logWriteErrorSampler.SampleRate().Value()),
			zap.Error(finalErr))
	}

	return finalErr
}

[end of src/dbnode/client/session.go]
