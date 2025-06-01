// Copyright (c) 2021 Uber Technologies, Inc.
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

package cluster

import (
	"context"

	"github.com/m3db/m3/src/dbnode/generated/proto/rpc"
	tchannelthriftcluster "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/cluster" // Placeholder for service.Service
	// NB: The actual service might be defined in a different package, e.g., `github.com/m3db/m3/src/cluster/services`
	// or similar, depending on where the core cluster logic resides.

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

	thriftRpc "github.com/m3db/m3/src/dbnode/generated/thrift/rpc" // For Thrift types
)

// ClusterServer implements the rpc.ClusterServer interface.
type ClusterServer struct {
	rpc.UnimplementedClusterServer
	service tchannelthriftcluster.Service // Placeholder for the actual M3DB cluster service
}

// NewClusterServer creates a new ClusterServer.
func NewClusterServer(service tchannelthriftcluster.Service) (*ClusterServer, error) {
	// TODO: Add any necessary validation or setup.
	return &ClusterServer{service: service}, nil
}

// --- Duplicated Helper Functions (from node/server.go) ---
// TODO: Refactor these into a shared package.

func toThriftTimeType(tt rpc.TimeType) thriftRpc.TimeType {
	switch tt {
	case rpc.TimeType_UNIX_SECONDS:
		return thriftRpc.TimeType_UNIX_SECONDS
	case rpc.TimeType_UNIX_MICROSECONDS:
		return thriftRpc.TimeType_UNIX_MICROSECONDS
	case rpc.TimeType_UNIX_MILLISECONDS:
		return thriftRpc.TimeType_UNIX_MILLISECONDS
	case rpc.TimeType_UNIX_NANOSECONDS:
		return thriftRpc.TimeType_UNIX_NANOSECONDS
	default:
		return thriftRpc.TimeType_UNIX_SECONDS // Default
	}
}

func fromThriftTimeType(tt thriftRpc.TimeType) rpc.TimeType {
	switch tt {
	case thriftRpc.TimeType_UNIX_SECONDS:
		return rpc.TimeType_UNIX_SECONDS
	case thriftRpc.TimeType_UNIX_MICROSECONDS:
		return rpc.TimeType_UNIX_MICROSECONDS
	case thriftRpc.TimeType_UNIX_MILLISECONDS:
		return rpc.TimeType_UNIX_MILLISECONDS
	case thriftRpc.TimeType_UNIX_NANOSECONDS:
		return rpc.TimeType_UNIX_NANOSECONDS
	default:
		return rpc.TimeType_UNIX_SECONDS // Default
	}
}

func toThriftDatapoint(dp *rpc.Datapoint) *thriftRpc.Datapoint {
	if dp == nil {
		return nil
	}
	return &thriftRpc.Datapoint{
		Timestamp:         dp.Timestamp,
		Value:             dp.Value,
		Annotation:        dp.Annotation,
		TimestampTimeType: toThriftTimeType(dp.TimestampTimeType),
	}
}

func fromThriftDatapoint(dp *thriftRpc.Datapoint) *rpc.Datapoint {
	if dp == nil {
		return nil
	}
	return &rpc.Datapoint{
		Timestamp:         dp.Timestamp,
		Value:             dp.Value,
		Annotation:        dp.Annotation,
		TimestampTimeType: fromThriftTimeType(dp.TimestampTimeType),
	}
}

func toThriftTag(tag *rpc.Tag) *thriftRpc.Tag {
	if tag == nil {
		return nil
	}
	return &thriftRpc.Tag{
		Name:  []byte(tag.Name),
		Value: []byte(tag.Value),
	}
}

func toThriftTags(protoTags []*rpc.Tag) []*thriftRpc.Tag {
	if protoTags == nil {
		return nil
	}
	thriftTags := make([]*thriftRpc.Tag, 0, len(protoTags))
	for _, t := range protoTags {
		thriftTags = append(thriftTags, toThriftTag(t))
	}
	return thriftTags
}

// fromThriftError converts a Thrift error to a gRPC status error.
// Note: This helper is duplicated. Ideally, it should be in a shared package.
func fromThriftError(err *thriftRpc.Error) error {
	if err == nil {
		return nil // Or some default error if appropriate
	}
	// Default to internal error
	grpcCode := codes.Internal
	errMsg := err.Message

	switch err.Type {
	case thriftRpc.ErrorType_BAD_REQUEST:
		grpcCode = codes.InvalidArgument
	case thriftRpc.ErrorType_INTERNAL_ERROR: // Already codes.Internal
	default:
		// Potentially log an unknown error type
	}
	// Consider adding err.Flags if relevant to the error message or code
	return status.Errorf(grpcCode, "cluster operation failed: %s", errMsg)
}

// --- End Duplicated Helper Functions ---

// Health implements the Health rpc endpoint for the cluster.
func (s *ClusterServer) Health(ctx context.Context, req *emptypb.Empty) (*rpc.HealthResult, error) {
	_ = req // req is not used

	// The tchannelthriftcluster.Service interface has Health(ctx context.Context) (*thriftRpc.HealthResult, error)
	health, err := s.service.Health(ctx)
	if err != nil {
		// Check if it's a thriftRpc.Error
		if te, ok := err.(*thriftRpc.Error); ok {
			return nil, fromThriftError(te)
		}
		return nil, status.Errorf(codes.Internal, "failed to get cluster health: %v", err)
	}

	return &rpc.HealthResult{
		Ok:     health.Ok,
		Status: health.Status,
	}, nil
}

// Write implements the Write rpc endpoint for the cluster.
func (s *ClusterServer) Write(ctx context.Context, req *rpc.WriteRequest) (*emptypb.Empty, error) {
	thriftReq := &thriftRpc.WriteRequest{
		NameSpace: []byte(req.NameSpace), // In cluster RPCs, these are often strings already in Thrift.
		ID:        []byte(req.Id),        // Let's check the thrift definition for Cluster service Write.
		Datapoint: toThriftDatapoint(req.Datapoint),
	}
	// From rpc.thrift: service Cluster { ... void write(1: WriteRequest req) ... }
	// WriteRequest is the same struct used by Node service (namespace string, id string).
	// So, the conversion from gRPC string to []byte for NameSpace and ID is correct if
	// the underlying service.Write is expecting the *node's* WriteRequest.
	// However, the tchannelthriftcluster.Service.Write method is:
	//   Write(ctx context.Context, req *rPcHdlr.WriteRequest) error
	// where rPcHdlr.WriteRequest has NameSpace and ID as strings.
	// So, conversion to []byte is NOT needed here.

	nodeWriteReq := req // Assuming the cluster service's Write method takes the *same* WriteRequest as node.
	                   // Let's re-verify the thrift definition for `Cluster.write`'s `WriteRequest`.
	                   // It is indeed `rpc.WriteRequest` which has `string nameSpace`, `string id`.
	                   // So, the gRPC `rpc.WriteRequest` fields `NameSpace` and `Id` (strings)
	                   // map directly to the Thrift `rpc.WriteRequest` fields.
	                   // The `toThriftDatapoint` helper is still valid.

	directThriftReq := &thriftRpc.WriteRequest{
		NameSpace: req.NameSpace, // string to string
		ID:        req.Id,        // string to string
		Datapoint: toThriftDatapoint(req.Datapoint),
	}


	err := s.service.Write(ctx, directThriftReq)
	if err != nil {
		if te, ok := err.(*thriftRpc.Error); ok {
			return nil, fromThriftError(te)
		}
		return nil, status.Errorf(codes.Internal, "cluster write failed: %v", err)
	}
	return &emptypb.Empty{}, nil
}

// WriteTagged implements the WriteTagged rpc endpoint for the cluster.
func (s *ClusterServer) WriteTagged(ctx context.Context, req *rpc.WriteTaggedRequest) (*emptypb.Empty, error) {
	// Similar to Write, Cluster.WriteTagged uses rpc.WriteTaggedRequest which has string NameSpace, ID.
	thriftReq := &thriftRpc.WriteTaggedRequest{
		NameSpace: req.NameSpace, // string to string
		ID:        req.Id,        // string to string
		Tags:      toThriftTags(req.Tags), // This helper converts gRPC Tag (string fields) to Thrift Tag (binary fields)
		Datapoint: toThriftDatapoint(req.Datapoint),
	}
	// The `toThriftTags` helper converts gRPC string tags to Thrift binary tags.
	// Let's verify if `rpc.WriteTaggedRequest`'s `Tags` field (`list<Tag> tags`)
	// where `Tag` is `struct Tag { 1: required string name, 2: required string value}`.
	// Yes, it is. So `toThriftTags` which expects `rpc.Tag` (string fields) and converts to `thriftRpc.Tag` (binary fields)
	// is what the *Node* service needed. But the *Cluster* service's WriteTagged also uses the *same* `rpc.WriteTaggedRequest`.
	// So, the Thrift `Tag` struct it expects also has string name/value.
	// This means `toThriftTags` as defined (converting to binary tags) is incorrect for this context.
	// We need a `toThriftStringTags` or adapt `toThriftTag`.

	// Corrected approach for Tags in Cluster.WriteTagged:
	// The `thriftRpc.Tag` struct (from `rpc.thrift`) has `string name` and `string value`.
	// The `rpc.Tag` (gRPC) also has `string name` and `string value`.
	// So, the conversion for tags should be direct.

	var directThriftTags []*thriftRpc.Tag
	if req.Tags != nil {
		directThriftTags = make([]*thriftRpc.Tag, 0, len(req.Tags))
		for _, protoTag := range req.Tags {
			if protoTag != nil {
				directThriftTags = append(directThriftTags, &thriftRpc.Tag{
					Name:  protoTag.Name,
					Value: protoTag.Value,
				})
			}
		}
	}

	correctedThriftReq := &thriftRpc.WriteTaggedRequest{
		NameSpace: req.NameSpace,
		ID:        req.Id,
		Tags:      directThriftTags,
		Datapoint: toThriftDatapoint(req.Datapoint),
	}

	err := s.service.WriteTagged(ctx, correctedThriftReq)
	if err != nil {
		if te, ok := err.(*thriftRpc.Error); ok {
			return nil, fromThriftError(te)
		}
		return nil, status.Errorf(codes.Internal, "cluster write tagged failed: %v", err)
	}
	return &emptypb.Empty{}, nil
}

// Fetch implements the Fetch rpc endpoint for the cluster.
func (s *ClusterServer) Fetch(ctx context.Context, req *rpc.FetchRequest) (*rpc.FetchResult, error) {
	// Cluster.Fetch uses rpc.FetchRequest (string NameSpace, string ID)
	thriftReq := &thriftRpc.FetchRequest{
		NameSpace:      req.NameSpace, // string to string
		ID:             req.Id,        // string to string
		RangeStart:     req.RangeStart,
		RangeEnd:       req.RangeEnd,
		RangeType:      toThriftTimeType(req.RangeType), // TimeType conversion is fine
		ResultTimeType: toThriftTimeType(req.ResultTimeType),
		Source:         req.Source, // Already bytes
	}

	thriftResult, err := s.service.Fetch(ctx, thriftReq)
	if err != nil {
		if te, ok := err.(*thriftRpc.Error); ok {
			return nil, fromThriftError(te)
		}
		return nil, status.Errorf(codes.Internal, "cluster fetch failed: %v", err)
	}

	protoDatapoints := make([]*rpc.Datapoint, 0, len(thriftResult.Datapoints))
	for _, dp := range thriftResult.Datapoints {
		protoDatapoints = append(protoDatapoints, fromThriftDatapoint(dp))
	}

	return &rpc.FetchResult{
		Datapoints: protoDatapoints,
	}, nil
}

// --- Enum Converters ---

func toThriftAggregateQueryType(aqt rpc.AggregateQueryType) thriftRpc.AggregateQueryType {
	switch aqt {
	case rpc.AggregateQueryType_AGGREGATE_BY_TAG_NAME:
		return thriftRpc.AggregateQueryType_AGGREGATE_BY_TAG_NAME
	case rpc.AggregateQueryType_AGGREGATE_BY_TAG_NAME_VALUE:
		return thriftRpc.AggregateQueryType_AGGREGATE_BY_TAG_NAME_VALUE
	default:
		// Default or error? The thrift definition has a default for this in AggregateQueryRequest.
		// Let's assume the service handles an unrecognized zero value if Go Thrift sends one.
		// Or, more robustly, map to the default specified in Thrift if proto sends its default (0).
		// For now, direct map.
		return thriftRpc.AggregateQueryType(aqt)
	}
}

func toThriftReadConsistency(rc rpc.ReadConsistency) thriftRpc.ReadConsistency {
	switch rc {
	case rpc.ReadConsistency_ONE:
		return thriftRpc.ReadConsistency_ONE
	case rpc.ReadConsistency_UNSTRICT_MAJORITY:
		return thriftRpc.ReadConsistency_UNSTRICT_MAJORITY
	case rpc.ReadConsistency_MAJORITY:
		return thriftRpc.ReadConsistency_MAJORITY
	case rpc.ReadConsistency_UNSTRICT_ALL:
		return thriftRpc.ReadConsistency_UNSTRICT_ALL
	case rpc.ReadConsistency_ALL:
		return thriftRpc.ReadConsistency_ALL
	default:
		return thriftRpc.ReadConsistency_ONE // Default, or handle error
	}
}

func toThriftEqualTimestampStrategy(ets rpc.EqualTimestampStrategy) thriftRpc.EqualTimestampStrategy {
	switch ets {
	case rpc.EqualTimestampStrategy_LAST_PUSHED:
		return thriftRpc.EqualTimestampStrategy_LAST_PUSHED
	case rpc.EqualTimestampStrategy_HIGHEST_VALUE:
		return thriftRpc.EqualTimestampStrategy_HIGHEST_VALUE
	case rpc.EqualTimestampStrategy_LOWEST_VALUE:
		return thriftRpc.EqualTimestampStrategy_LOWEST_VALUE
	case rpc.EqualTimestampStrategy_HIGHEST_FREQUENCY:
		return thriftRpc.EqualTimestampStrategy_HIGHEST_FREQUENCY
	default:
		return thriftRpc.EqualTimestampStrategy_LAST_PUSHED // Default, or handle error
	}
}

// --- Request Converters ---

func toThriftClusterQueryOptions(opts *rpc.ClusterQueryOptions) *thriftRpc.ClusterQueryOptions {
	if opts == nil {
		return nil
	}
	// Thrift ClusterQueryOptions has optional fields, so pointers are expected.
	thriftOpts := &thriftRpc.ClusterQueryOptions{}
	// Assuming direct enum conversion is fine and service handles zero values if not explicitly set.
	// Or, if gRPC default enum (0) is meaningful, it will be passed.
	// The thrift definition uses "optional" for these enums.
	// If the gRPC enum value is the default (0), we might choose not to set the field
	// in the thrift struct if it's a pointer, to let thrift's own defaults apply.
	// However, since the enum values start from 0 and are all valid, we set them.
	// This means the default specified in thrift (if any for optional enums) might be overridden.
	rc := toThriftReadConsistency(opts.ReadConsistency)
	thriftOpts.ReadConsistency = &rc
	ets := toThriftEqualTimestampStrategy(opts.ConflictResolutionStrategy)
	thriftOpts.ConflictResolutionStrategy = &ets
	return thriftOpts
}

func toThriftQuery(q *rpc.Query) *thriftRpc.Query {
	if q == nil {
		return nil
	}
	thrift_q := &thriftRpc.Query{}
	switch query := q.Query.(type) {
	case *rpc.Query_Term:
		thrift_q.Term = &thriftRpc.TermQuery{Field: query.Term.Field, Term: query.Term.Term}
	case *rpc.Query_Regexp:
		thrift_q.Regexp = &thriftRpc.RegexpQuery{Field: query.Regexp.Field, Regexp: query.Regexp.Regexp}
	case *rpc.Query_Negation:
		thrift_q.Negation = &thriftRpc.NegationQuery{Query: toThriftQuery(query.Negation.Query)}
	case *rpc.Query_Conjunction:
		subQueries := make([]*thriftRpc.Query, 0, len(query.Conjunction.Queries))
		for _, sub_q := range query.Conjunction.Queries {
			subQueries = append(subQueries, toThriftQuery(sub_q))
		}
		thrift_q.Conjunction = &thriftRpc.ConjunctionQuery{Queries: subQueries}
	case *rpc.Query_Disjunction:
		subQueries := make([]*thriftRpc.Query, 0, len(query.Disjunction.Queries))
		for _, sub_q := range query.Disjunction.Queries {
			subQueries = append(subQueries, toThriftQuery(sub_q))
		}
		thrift_q.Disjunction = &thriftRpc.DisjunctionQuery{Queries: subQueries}
	case *rpc.Query_All:
		thrift_q.All = &thriftRpc.AllQuery{}
	case *rpc.Query_Field:
		thrift_q.Field = &thriftRpc.FieldQuery{Field: query.Field.Field}
	}
	return thrift_q
}

// --- Response Converters ---

// Helper for Tag conversion (string fields to string fields for Cluster context)
func fromThriftStringTag(tag *thriftRpc.Tag) *rpc.Tag {
    if tag == nil {
        return nil
    }
    return &rpc.Tag{
        Name:  tag.Name,
        Value: tag.Value,
    }
}

func fromThriftStringTags(thriftTags []*thriftRpc.Tag) []*rpc.Tag {
    if thriftTags == nil {
        return nil
    }
    protoTags := make([]*rpc.Tag, 0, len(thriftTags))
    for _, t := range thriftTags {
        protoTags = append(protoTags, fromThriftStringTag(t))
    }
    return protoTags
}


func fromThriftQueryResultElement(elem *thriftRpc.QueryResultElement) *rpc.QueryResultElement {
	if elem == nil {
		return nil
	}
	datapoints := make([]*rpc.Datapoint, 0, len(elem.Datapoints))
	for _, dp := range elem.Datapoints {
		datapoints = append(datapoints, fromThriftDatapoint(dp))
	}
	return &rpc.QueryResultElement{
		Id:         elem.ID,
		Tags:       fromThriftStringTags(elem.Tags), // Using string tags for cluster results
		Datapoints: datapoints,
	}
}

func fromThriftAggregateQueryResultTagValueElement(elem *thriftRpc.AggregateQueryResultTagValueElement) *rpc.AggregateQueryResultTagValueElement {
	if elem == nil {
		return nil
	}
	return &rpc.AggregateQueryResultTagValueElement{
		TagValue: elem.TagValue,
	}
}

func fromThriftAggregateQueryResultTagNameElement(elem *thriftRpc.AggregateQueryResultTagNameElement) *rpc.AggregateQueryResultTagNameElement {
	if elem == nil {
		return nil
	}
	tagValues := make([]*rpc.AggregateQueryResultTagValueElement, 0, len(elem.TagValues))
	for _, tv := range elem.TagValues {
		tagValues = append(tagValues, fromThriftAggregateQueryResultTagValueElement(tv))
	}
	return &rpc.AggregateQueryResultTagNameElement{
		TagName:   elem.TagName,
		TagValues: tagValues,
	}
}


// Query implements the Query rpc endpoint for the cluster.
func (s *ClusterServer) Query(ctx context.Context, req *rpc.QueryRequest) (*rpc.QueryResult, error) {
	thriftReq := &thriftRpc.QueryRequest{
		Query:          toThriftQuery(req.Query),
		RangeStart:     req.RangeStart,
		RangeEnd:       req.RangeEnd,
		NameSpace:      req.NameSpace, // string
		Limit:          req.Limit,
		NoData:         &req.NoData, // Thrift QueryRequest has NoData as *bool
		RangeType:      toThriftTimeType(req.RangeType),
		ResultTimeType: toThriftTimeType(req.ResultTimeType),
		Source:         req.Source,
		ClusterOptions: toThriftClusterQueryOptions(req.ClusterOptions),
	}
	// Note on req.NoData: gRPC bool defaults to false. If Thrift service needs to distinguish
	// "not set" from "explicitly false", this direct pointer assignment might not be enough.
	// However, for bools, usually false means the feature is off, so it's often fine.

	thriftResult, err := s.service.Query(ctx, thriftReq)
	if err != nil {
		if te, ok := err.(*thriftRpc.Error); ok {
			return nil, fromThriftError(te)
		}
		return nil, status.Errorf(codes.Internal, "cluster query failed: %v", err)
	}

	results := make([]*rpc.QueryResultElement, 0, len(thriftResult.Results))
	for _, elem := range thriftResult.Results {
		results = append(results, fromThriftQueryResultElement(elem))
	}

	return &rpc.QueryResult{
		Results:    results,
		Exhaustive: thriftResult.Exhaustive,
	}, nil
}

// Aggregate implements the Aggregate rpc endpoint for the cluster.
func (s *ClusterServer) Aggregate(ctx context.Context, req *rpc.AggregateQueryRequest) (*rpc.AggregateQueryResult, error) {
	// The thrift AggregateQueryRequest has 'RequireExhaustive' and 'RequireNoWait' as optional bools.
	// These will be *bool in the generated Go Thrift struct.
	var requireExhaustive *bool
	if req.RequireExhaustive { // if true (non-default for proto bool), set it. If false, Thrift might have its own default.
		requireExhaustive = &req.RequireExhaustive
	}
	var requireNoWait *bool
	if req.RequireNoWait {
		requireNoWait = &req.RequireNoWait
	}
	// This handling of optional bools is basic. If Thrift service distinguishes "unset" from "false",
	// gRPC protos would ideally use wrappers like BoolValue.

	thriftReq := &thriftRpc.AggregateQueryRequest{
		Query:              toThriftQuery(req.Query),
		RangeStart:         req.RangeStart,
		RangeEnd:           req.RangeEnd,
		NameSpace:          req.NameSpace, // string
		SeriesLimit:        req.SeriesLimit,
		TagNameFilter:      req.TagNameFilter, // list<string> to list<string>
		AggregateQueryType: toThriftAggregateQueryType(req.AggregateQueryType),
		RangeType:          toThriftTimeType(req.RangeType),
		Source:             req.Source,
		DocsLimit:          req.DocsLimit,
		RequireExhaustive:  requireExhaustive,
		RequireNoWait:      requireNoWait,
	}

	thriftResult, err := s.service.Aggregate(ctx, thriftReq)
	if err != nil {
		if te, ok := err.(*thriftRpc.Error); ok {
			return nil, fromThriftError(te)
		}
		return nil, status.Errorf(codes.Internal, "cluster aggregate failed: %v", err)
	}

	results := make([]*rpc.AggregateQueryResultTagNameElement, 0, len(thriftResult.Results))
	for _, elem := range thriftResult.Results {
		results = append(results, fromThriftAggregateQueryResultTagNameElement(elem))
	}

	return &rpc.AggregateQueryResult{
		Results:    results,
		Exhaustive: thriftResult.Exhaustive,
	}, nil
}

// Truncate implements the Truncate rpc endpoint for the cluster.
func (s *ClusterServer) Truncate(ctx context.Context, req *rpc.TruncateRequest) (*rpc.TruncateResult, error) {
	// rpc.thrift definition for Cluster.Truncate:
	// TruncateResult truncate(1: TruncateRequest req)
	// TruncateRequest is defined as:
	// struct TruncateRequest {
	//   1: required binary nameSpace
	// }
	// The gRPC rpc.TruncateRequest has `string name_space`. So, conversion is needed.
	thriftReq := &thriftRpc.TruncateRequest{
		NameSpace: []byte(req.NameSpace),
	}

	thriftResult, err := s.service.Truncate(ctx, thriftReq)
	if err != nil {
		if te, ok := err.(*thriftRpc.Error); ok {
			return nil, fromThriftError(te)
		}
		return nil, status.Errorf(codes.Internal, "cluster truncate failed: %v", err)
	}

	return &rpc.TruncateResult{
		NumSeries: thriftResult.NumSeries,
	}, nil
}
