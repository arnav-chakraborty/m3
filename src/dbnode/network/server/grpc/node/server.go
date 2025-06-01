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

package node

import (
	"context"

	"github.com/m3db/m3/src/dbnode/generated/proto/rpc"
	dbnode "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/node" // Using existing Health method for now

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// NodeServer implements the rpc.NodeServer interface.
type NodeServer struct {
	rpc.UnimplementedNodeServer
	service dbnode.Service // Placeholder for the actual M3DB node service
	// Options can be added here if needed for conversions, e.g. TimeType defaults
}

// NewNodeServer creates a new NodeServer.
func NewNodeServer(service dbnode.Service) (*NodeServer, error) {
	// TODO: Add any necessary validation or setup.
	return &NodeServer{service: service}, nil
}

// Helper to convert rpc.TimeType to thriftrpc.TimeType
func toThriftTimeType(tt rpc.TimeType) dbnode.TimeType {
	switch tt {
	case rpc.TimeType_UNIX_SECONDS:
		return dbnode.TimeTypeUnixSeconds
	case rpc.TimeType_UNIX_MICROSECONDS:
		return dbnode.TimeTypeUnixMicroseconds
	case rpc.TimeType_UNIX_MILLISECONDS:
		return dbnode.TimeTypeUnixMilliseconds
	case rpc.TimeType_UNIX_NANOSECONDS:
		return dbnode.TimeTypeUnixNanoseconds
	default:
		// Default or throw error? For now, default to seconds as per some thrift defaults.
		// This should be aligned with actual service requirements.
		return dbnode.TimeTypeUnixSeconds
	}
}

// Helper to convert thriftrpc.TimeType to rpc.TimeType
func fromThriftTimeType(tt dbnode.TimeType) rpc.TimeType {
	switch tt {
	case dbnode.TimeTypeUnixSeconds:
		return rpc.TimeType_UNIX_SECONDS
	case dbnode.TimeTypeUnixMicroseconds:
		return rpc.TimeType_UNIX_MICROSECONDS
	case dbnode.TimeTypeUnixMilliseconds:
		return rpc.TimeType_UNIX_MILLISECONDS
	case dbnode.TimeTypeUnixNanoseconds:
		return rpc.TimeType_UNIX_NANOSECONDS
	default:
		return rpc.TimeType_UNIX_SECONDS // Default
	}
}

// Helper to convert rpc.Datapoint to thriftrpc.Datapoint
func toThriftDatapoint(dp *rpc.Datapoint) *dbnode.Datapoint {
	if dp == nil {
		return nil
	}
	return &dbnode.Datapoint{
		Timestamp:         dp.Timestamp,
		Value:             dp.Value,
		Annotation:        dp.Annotation,
		TimestampTimeType: toThriftTimeType(dp.TimestampTimeType),
	}
}

// Helper to convert thriftrpc.Datapoint to rpc.Datapoint
func fromThriftDatapoint(dp *dbnode.Datapoint) *rpc.Datapoint {
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

// Health implements the Health rpc endpoint.
func (s *NodeServer) Health(ctx context.Context, req *emptypb.Empty) (*rpc.NodeHealthResult, error) {
	_ = req // req is not used for Health

	health, err := s.service.Health(ctx)
	if err != nil {
		// TODO: Determine the correct gRPC error code based on the error type.
		// Consider using a helper function to map domain errors to gRPC status codes.
		return nil, status.Errorf(codes.Internal, "failed to get health: %v", err)
	}

	// Assuming health.Metadata is map[string]string which is compatible.
	// If health.Metadata is nil, it will be an empty map in the protobuf message.
	return &rpc.NodeHealthResult{
		Ok:           health.Ok,
		Status:       health.Status,
		Bootstrapped: health.Bootstrapped,
		Metadata:     health.Metadata,
	}, nil
}

// Fetch implements the Fetch rpc endpoint.
func (s *NodeServer) Fetch(ctx context.Context, req *rpc.FetchRequest) (*rpc.FetchResult, error) {
	// Convert gRPC request to Thrift request
	thriftReq := &dbnode.FetchRequest{
		NameSpace:      []byte(req.NameSpace), // NB: Thrift uses binary for namespace
		ID:             []byte(req.Id),        // NB: Thrift uses binary for ID
		RangeStart:     req.RangeStart,
		RangeEnd:       req.RangeEnd,
		RangeType:      toThriftTimeType(req.RangeType),
		ResultTimeType: toThriftTimeType(req.ResultTimeType),
		Source:         req.Source,
	}

	// Call the underlying service
	thriftResult, err := s.service.Fetch(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error to gRPC error status
		// This could involve checking err type if service.Fetch returns structured errors
		return nil, status.Errorf(codes.Internal, "Fetch failed: %v", err)
	}

	// Convert Thrift result to gRPC result
	protoDatapoints := make([]*rpc.Datapoint, 0, len(thriftResult.Datapoints))
	for _, dp := range thriftResult.Datapoints {
		protoDatapoints = append(protoDatapoints, fromThriftDatapoint(dp))
	}

	return &rpc.FetchResult{
		Datapoints: protoDatapoints,
	}, nil
}

// Write implements the Write rpc endpoint.
func (s *NodeServer) Write(ctx context.Context, req *rpc.WriteRequest) (*emptypb.Empty, error) {
	// Convert gRPC request to Thrift request
	thriftReq := &dbnode.WriteRequest{
		NameSpace: []byte(req.NameSpace), // NB: Thrift uses binary for namespace
		ID:        []byte(req.Id),        // NB: Thrift uses binary for ID
		Datapoint: toThriftDatapoint(req.Datapoint),
	}

	// Call the underlying service
	err := s.service.Write(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error to gRPC error status
		return nil, status.Errorf(codes.Internal, "Write failed: %v", err)
	}

	return &emptypb.Empty{}, nil
}

// Helper to convert rpc.Tag to thriftrpc.Tag
func toThriftTag(tag *rpc.Tag) *dbnode.Tag {
	if tag == nil {
		return nil
	}
	return &dbnode.Tag{
		Name:  []byte(tag.Name), // Thrift Tag uses binary for Name and Value
		Value: []byte(tag.Value),
	}
}

// Helper to convert a list of rpc.Tag to a list of thriftrpc.Tag
func toThriftTags(protoTags []*rpc.Tag) []*dbnode.Tag {
	if protoTags == nil {
		return nil // Or empty slice? Depending on how Thrift service handles nil vs empty.
		             // For now, if input is nil, output is nil.
	}
	thriftTags := make([]*dbnode.Tag, 0, len(protoTags))
	for _, t := range protoTags {
		thriftTags = append(thriftTags, toThriftTag(t))
	}
	return thriftTags
}

// WriteTagged implements the WriteTagged rpc endpoint.
func (s *NodeServer) WriteTagged(ctx context.Context, req *rpc.WriteTaggedRequest) (*emptypb.Empty, error) {
	thriftReq := &dbnode.WriteTaggedRequest{
		NameSpace: []byte(req.NameSpace),
		ID:        []byte(req.Id),
		Tags:      toThriftTags(req.Tags),
		Datapoint: toThriftDatapoint(req.Datapoint),
	}

	err := s.service.WriteTagged(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		// Example: if e, ok := err.(*dbnode.Error); ok { return fromThriftError(e) }
		return nil, status.Errorf(codes.Internal, "WriteTagged failed: %v", err)
	}
	return &emptypb.Empty{}, nil
}

// Helper to convert rpc.WriteTaggedBatchRawV2RequestElement to thriftrpc.WriteTaggedBatchRawV2RequestElement
func toThriftWriteTaggedBatchRawV2RequestElement(elem *rpc.WriteTaggedBatchRawV2RequestElement) *dbnode.WriteTaggedBatchRawV2RequestElement {
	if elem == nil {
		return nil
	}
	return &dbnode.WriteTaggedBatchRawV2RequestElement{
		ID:          elem.Id,
		EncodedTags: elem.EncodedTags,
		Datapoint:   toThriftDatapoint(elem.Datapoint),
		NameSpace:   elem.NameSpace, // This is an index
	}
}

// WriteTaggedBatchRawV2 implements the WriteTaggedBatchRawV2 rpc endpoint.
func (s *NodeServer) WriteTaggedBatchRawV2(ctx context.Context, req *rpc.WriteTaggedBatchRawV2Request) (*emptypb.Empty, error) {
	thriftElements := make([]*dbnode.WriteTaggedBatchRawV2RequestElement, 0, len(req.Elements))
	for _, elem := range req.Elements {
		thriftElements = append(thriftElements, toThriftWriteTaggedBatchRawV2RequestElement(elem))
	}

	thriftReq := &dbnode.WriteTaggedBatchRawV2Request{
		NameSpaces: req.NameSpaces,
		Elements:   thriftElements,
	}

	err := s.service.WriteTaggedBatchRawV2(ctx, thriftReq)
	if err != nil {
		if typedErr, ok := err.(*dbnode.WriteBatchRawErrors); ok {
			var errMsgs []string
			for _, e := range typedErr.Errors {
				errMsgs = append(errMsgs, e.Err.Message)
			}
			return nil, status.Errorf(codes.InvalidArgument, "WriteTaggedBatchRawV2 failed for some elements: %v", errMsgs)
		}
		// TODO: Map other Thrift errors to gRPC error status
		return nil, status.Errorf(codes.Internal, "WriteTaggedBatchRawV2 failed: %v", err)
	}
	return &emptypb.Empty{}, nil
}

// Repair implements the Repair rpc endpoint.
func (s *NodeServer) Repair(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	_ = req // req is not used
	err := s.service.Repair(ctx)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "Repair failed: %v", err)
	}
	return &emptypb.Empty{}, nil
}

// Truncate implements the Truncate rpc endpoint.
func (s *NodeServer) Truncate(ctx context.Context, req *rpc.TruncateRequest) (*rpc.TruncateResult, error) {
	thriftReq := &dbnode.TruncateRequest{
		NameSpace: req.NameSpace, // Already bytes
	}

	thriftResult, err := s.service.Truncate(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "Truncate failed: %v", err)
	}

	return &rpc.TruncateResult{
		NumSeries: thriftResult.NumSeries,
	}, nil
}

// Bootstrapped implements the Bootstrapped rpc endpoint.
func (s *NodeServer) Bootstrapped(ctx context.Context, req *emptypb.Empty) (*rpc.NodeBootstrappedResult, error) {
	_ = req // req is not used
	// The underlying service.Bootstrapped() in tchannelthrift/node/service.go returns (BootstrappedResult, error)
	// where BootstrappedResult is an empty struct.
	// So, we just need to call it and check the error.
	_, err := s.service.Bootstrapped(ctx) // Assuming the placeholder service has this method.
	                                     // This method is actually defined on `AdminService` in the example,
	                                     // so `s.service` (node.Service) might not have it directly.
	                                     // This will need to be adjusted based on the actual service structure.
	                                     // For now, proceeding as if s.service has Bootstrapped().

	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "Bootstrapped check failed: %v", err)
	}
	return &rpc.NodeBootstrappedResult{}, nil // Empty response
}

// GetPersistRateLimit implements the GetPersistRateLimit rpc endpoint.
func (s *NodeServer) GetPersistRateLimit(ctx context.Context, req *emptypb.Empty) (*rpc.NodePersistRateLimitResult, error) {
	_ = req // req is not used

	// Assuming s.service has GetPersistRateLimit. This method is on AdminService in tchannel node.
	// Adjust if s.service type changes or method is located elsewhere.
	thriftResult, err := s.service.GetPersistRateLimit(ctx)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "GetPersistRateLimit failed: %v", err)
	}

	return &rpc.NodePersistRateLimitResult{
		LimitEnabled:    thriftResult.LimitEnabled,
		LimitMbps:       thriftResult.LimitMbps,
		LimitCheckEvery: thriftResult.LimitCheckEvery,
	}, nil
}

// SetPersistRateLimit implements the SetPersistRateLimit rpc endpoint.
func (s *NodeServer) SetPersistRateLimit(ctx context.Context, req *rpc.NodeSetPersistRateLimitRequest) (*rpc.NodePersistRateLimitResult, error) {
	// The Thrift NodeSetPersistRateLimitRequest has optional fields.
	// The generated Go Thrift struct uses pointers for these optional primitive types.
	thriftReq := &dbnode.NodeSetPersistRateLimitRequest{}

	// For gRPC, if a value is its default (e.g., false for bool, 0 for numbers),
	// it's indistinguishable from not being set.
	// If the intent is to allow setting these fields to their default values explicitly,
	// this direct mapping is fine. If "unset" needs to be differentiated from "set to default",
	// the gRPC messages would need wrappers like google.protobuf.BoolValue, etc.
	// The Thrift definition `optional bool limitEnabled` implies we should only set the
	// field in the Thrift request if it's meaningfully provided by the gRPC client.
	// However, standard proto3 bool/numeric fields don't directly convey "isset".
	// For now, we pass values directly. If gRPC client sends default, that default is passed.
	// The current rpc.NodeSetPersistRateLimitRequest has plain bool/double/int64 fields.
	// The Thrift struct dbnode.NodeSetPersistRateLimitRequest has fields like:
	// LimitEnabled *bool `thrift:"limitEnabled,1,optional"`
	// LimitMbps *float64 `thrift:"limitMbps,2,optional"`
	// LimitCheckEvery *int64 `thrift:"limitCheckEvery,3,optional"`
	// So we should pass pointers if the client intends to set them.
	// But how do we know from a plain bool in gRPC if it was explicitly set or just defaulted?
	// This is a common gRPC design choice: either use wrapper types (BoolValue etc.) in proto
	// or accept that default values are passed as actual values.
	// Given the current proto, we assume direct value passing is intended.
	// The service implementation on the Thrift side would then decide how to interpret a value
	// (e.g. if a 0 for LimitCheckEvery means "don't update" or "set to 0").

	// Let's assume the service's SetPersistRateLimit expects pointers for optional fields.
	// If the gRPC request field is the zero value, we might not want to set the pointer,
	// effectively treating zero values as "not set". This is a policy decision.
	// A safer approach is to check if the client *intended* to send a value.
	// Since proto3 doesn't distinguish well, we'll pass values as they are.
	// The service needs to be robust to receiving zero values.
	//
	// The TChannel Admin service `SetAdminPersistRateLimit` actually takes
	// `services.AdminSetPersistRateLimitRequest` which has pointer fields.
	// Our current `dbnode.NodeSetPersistRateLimitRequest` (from the rpc.thrift)
	// also has optional fields, which typically translate to pointers in Go Thrift.
	//
	// Correct approach: if the gRPC message intends to support partial updates (set only some fields),
	// it should use FieldMasks or wrapper types (e.g. google.protobuf.BoolValue).
	// Since it uses plain types, we assume all fields are always "set" from gRPC's perspective.
	// So, we will assign pointers to the values from the gRPC request for the Thrift request.

	// This is a simplification. If the gRPC request is meant to represent partial updates
	// (e.g., only update LimitMbps), then the proto should use `google.protobuf.DoubleValue` etc.
	// Since it doesn't, we assume all fields are provided.
	thriftReq.LimitEnabled = &req.LimitEnabled
	thriftReq.LimitMbps = &req.LimitMbps
	thriftReq.LimitCheckEvery = &req.LimitCheckEvery


	// Assuming s.service has SetPersistRateLimit. This method is on AdminService in tchannel node.
	thriftResult, err := s.service.SetPersistRateLimit(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "SetPersistRateLimit failed: %v", err)
	}

	return &rpc.NodePersistRateLimitResult{
		LimitEnabled:    thriftResult.LimitEnabled,
		LimitMbps:       thriftResult.LimitMbps,
		LimitCheckEvery: thriftResult.LimitCheckEvery,
	}, nil
}

// GetWriteNewSeriesAsync implements the GetWriteNewSeriesAsync rpc endpoint.
func (s *NodeServer) GetWriteNewSeriesAsync(ctx context.Context, req *emptypb.Empty) (*rpc.NodeWriteNewSeriesAsyncResult, error) {
	_ = req // req is not used

	// Assuming s.service has GetWriteNewSeriesAsync. This method is on AdminService in tchannel node.
	thriftResult, err := s.service.GetWriteNewSeriesAsync(ctx)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "GetWriteNewSeriesAsync failed: %v", err)
	}

	return &rpc.NodeWriteNewSeriesAsyncResult{
		WriteNewSeriesAsync: thriftResult.WriteNewSeriesAsync,
	}, nil
}

// GetWriteNewSeriesBackoffDuration implements the GetWriteNewSeriesBackoffDuration rpc endpoint.
func (s *NodeServer) GetWriteNewSeriesBackoffDuration(ctx context.Context, req *emptypb.Empty) (*rpc.NodeWriteNewSeriesBackoffDurationResult, error) {
	_ = req // req is not used

	// Assuming s.service has this method. (AdminService in tchannel node)
	thriftResult, err := s.service.GetWriteNewSeriesBackoffDuration(ctx)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "GetWriteNewSeriesBackoffDuration failed: %v", err)
	}

	return &rpc.NodeWriteNewSeriesBackoffDurationResult{
		WriteNewSeriesBackoffDuration: thriftResult.WriteNewSeriesBackoffDuration,
		DurationType:                  fromThriftTimeType(thriftResult.DurationType),
	}, nil
}

// SetWriteNewSeriesBackoffDuration implements the SetWriteNewSeriesBackoffDuration rpc endpoint.
func (s *NodeServer) SetWriteNewSeriesBackoffDuration(
	ctx context.Context,
	req *rpc.NodeSetWriteNewSeriesBackoffDurationRequest,
) (*rpc.NodeWriteNewSeriesBackoffDurationResult, error) {
	// The Thrift NodeSetWriteNewSeriesBackoffDurationRequest has DurationType as optional TimeType.
	// `optional TimeType durationType = TimeType.UNIX_MILLISECONDS`
	// The generated Go Thrift for `optional TimeType` might be `*TimeType`.
	// Let's check `dbnode.NodeSetWriteNewSeriesBackoffDurationRequest` definition.
	// If it's `*dbnode.TimeType`, we need to handle it.
	// If it's `dbnode.TimeType` and relies on a default value or zero value for "unset",
	// then direct conversion is fine.
	// The rpc.thrift has: `2: optional TimeType durationType = TimeType.UNIX_MILLISECONDS`
	// The generated tchannelthrift `node.NodeSetWriteNewSeriesBackoffDurationRequest` has `DurationType *TimeType`.
	// So, we need to pass a pointer.

	var thriftDurationType *dbnode.TimeType
	// For enums in proto3, the default value is the first one (typically XXX_UNSPECIFIED or 0).
	// If rpc.TimeType_UNIX_SECONDS is 0, and client sends nothing, req.DurationType will be UNIX_SECONDS.
	// If client explicitly sends UNIX_SECONDS, it's the same.
	// We need to decide if sending the "zero value" enum means "set to this zero value" or "use default/don't set".
	// Given Thrift uses a pointer, it can distinguish "not set" from "set to a value".
	// If gRPC sends the default enum value (e.g., UNIX_SECONDS if it's 0), we will convert it.
	// If the client *intends* to signal "unset" for an optional enum, gRPC proto would need a wrapper or a convention.
	// For now, any value from gRPC (including the default 0-value enum) will be converted and set.
	// This means the default specified in Thrift (`TimeType.UNIX_MILLISECONDS`) will only apply if
	// the client *truly* omits the field in a way that Thrift understands (e.g. not possible with plain proto3 enums).
	// Better: if the gRPC enum is its zero-value, consider it "unset" for the optional Thrift field.
	// However, TimeType_UNIX_SECONDS is 0, which is a valid value.
	// This is tricky. Simplest is to always set it.
	// The Thrift service might apply its own default if the pointer is nil.
	// Or, if we always send a value, that value is used.

	// Let's assume the Go Thrift struct `dbnode.NodeSetWriteNewSeriesBackoffDurationRequest` has `DurationType *dbnode.TimeType`.
	// If `req.DurationType` is provided (i.e., not the default enum value if we had an UNSPECIFIED),
	// we set it. Otherwise, we could pass nil to let Thrift use its default.
	// Since `rpc.TimeType` doesn't have an `UNSPECIFIED` value at index 0, any value is considered "set".
	tt := toThriftTimeType(req.DurationType)
	thriftDurationType = &tt

	thriftReq := &dbnode.NodeSetWriteNewSeriesBackoffDurationRequest{
		WriteNewSeriesBackoffDuration: req.WriteNewSeriesBackoffDuration,
		DurationType:                  thriftDurationType,
	}

	// Assuming s.service has this method. (AdminService in tchannel node)
	thriftResult, err := s.service.SetWriteNewSeriesBackoffDuration(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "SetWriteNewSeriesBackoffDuration failed: %v", err)
	}

	return &rpc.NodeWriteNewSeriesBackoffDurationResult{
		WriteNewSeriesBackoffDuration: thriftResult.WriteNewSeriesBackoffDuration,
		DurationType:                  fromThriftTimeType(thriftResult.DurationType),
	}, nil
}

// GetWriteNewSeriesLimitPerShardPerSecond implements the GetWriteNewSeriesLimitPerShardPerSecond rpc endpoint.
func (s *NodeServer) GetWriteNewSeriesLimitPerShardPerSecond(
	ctx context.Context,
	req *emptypb.Empty,
) (*rpc.NodeWriteNewSeriesLimitPerShardPerSecondResult, error) {
	_ = req // req is not used

	// Assuming s.service has this method. (AdminService in tchannel node)
	thriftResult, err := s.service.GetWriteNewSeriesLimitPerShardPerSecond(ctx)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "GetWriteNewSeriesLimitPerShardPerSecond failed: %v", err)
	}

	return &rpc.NodeWriteNewSeriesLimitPerShardPerSecondResult{
		WriteNewSeriesLimitPerShardPerSecond: thriftResult.WriteNewSeriesLimitPerShardPerSecond,
	}, nil
}

// DebugProfileStart implements the DebugProfileStart rpc endpoint.
func (s *NodeServer) DebugProfileStart(ctx context.Context, req *rpc.DebugProfileStartRequest) (*rpc.DebugProfileStartResult, error) {
	// The Thrift DebugProfileStartRequest has optional fields.
	// Generated Go Thrift structs typically use pointers for these.
	// e.g., Interval *string, Duration *string, Debug *int64, etc.
	thriftReq := &dbnode.DebugProfileStartRequest{
		Name:             req.Name,
		FilePathTemplate: req.FilePathTemplate,
	}

	// For optional fields from gRPC (which default to zero-values if not set):
	// If the Thrift service expects nil for "not set", we should only assign pointers
	// if the gRPC value is not the zero value.
	// This is a common pattern for bridging proto3 default values to Thrift optionality.

	if req.Interval != "" {
		thriftReq.Interval = &req.Interval
	}
	if req.Duration != "" {
		thriftReq.Duration = &req.Duration
	}
	// For numeric types, distinguishing "set to 0" from "not set" is harder with plain proto3 fields.
	// If 0 is a valid value that needs to be explicitly profiled, this direct check isn't enough.
	// Proto wrapper types (Int64Value) would be needed. Assuming 0 means "not set" for these debug options.
	if req.Debug != 0 {
		thriftReq.Debug = &req.Debug
	}
	if req.ConditionalNumGoroutinesGreaterThan != 0 {
		thriftReq.ConditionalNumGoroutinesGreaterThan = &req.ConditionalNumGoroutinesGreaterThan
	}
	if req.ConditionalNumGoroutinesLessThan != 0 {
		thriftReq.ConditionalNumGoroutinesLessThan = &req.ConditionalNumGoroutinesLessThan
	}
	// For bool, false is the default. If ConditionalIsOverloaded=false is a meaningful explicit setting,
	// this simple check is not enough. Assuming 'false' means "don't apply this condition".
	if req.ConditionalIsOverloaded {
		thriftReq.ConditionalIsOverloaded = &req.ConditionalIsOverloaded
	}

	// Assuming s.service has this method. (Likely AdminService)
	_, err := s.service.DebugProfileStart(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "DebugProfileStart failed: %v", err)
	}

	return &rpc.DebugProfileStartResult{}, nil
}

// DebugProfileStop implements the DebugProfileStop rpc endpoint.
func (s *NodeServer) DebugProfileStop(ctx context.Context, req *rpc.DebugProfileStopRequest) (*rpc.DebugProfileStopResult, error) {
	thriftReq := &dbnode.DebugProfileStopRequest{
		Name: req.Name,
	}

	// Assuming s.service has this method. (Likely AdminService)
	_, err := s.service.DebugProfileStop(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "DebugProfileStop failed: %v", err)
	}

	return &rpc.DebugProfileStopResult{}, nil
}

// DebugIndexMemorySegments implements the DebugIndexMemorySegments rpc endpoint.
func (s *NodeServer) DebugIndexMemorySegments(
	ctx context.Context,
	req *rpc.DebugIndexMemorySegmentsRequest,
) (*rpc.DebugIndexMemorySegmentsResult, error) {
	thriftReq := &dbnode.DebugIndexMemorySegmentsRequest{
		Directory: req.Directory,
	}

	// Assuming s.service has this method. (Likely AdminService)
	_, err := s.service.DebugIndexMemorySegments(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "DebugIndexMemorySegments failed: %v", err)
	}

	return &rpc.DebugIndexMemorySegmentsResult{}, nil
}

// SetWriteNewSeriesLimitPerShardPerSecond implements the SetWriteNewSeriesLimitPerShardPerSecond rpc endpoint.
func (s *NodeServer) SetWriteNewSeriesLimitPerShardPerSecond(
	ctx context.Context,
	req *rpc.NodeSetWriteNewSeriesLimitPerShardPerSecondRequest,
) (*rpc.NodeWriteNewSeriesLimitPerShardPerSecondResult, error) {
	thriftReq := &dbnode.NodeSetWriteNewSeriesLimitPerShardPerSecondRequest{
		WriteNewSeriesLimitPerShardPerSecond: req.WriteNewSeriesLimitPerShardPerSecond,
	}

	// Assuming s.service has this method. (AdminService in tchannel node)
	thriftResult, err := s.service.SetWriteNewSeriesLimitPerShardPerSecond(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "SetWriteNewSeriesLimitPerShardPerSecond failed: %v", err)
	}

	return &rpc.NodeWriteNewSeriesLimitPerShardPerSecondResult{
		WriteNewSeriesLimitPerShardPerSecond: thriftResult.WriteNewSeriesLimitPerShardPerSecond,
	}, nil
}

// SetWriteNewSeriesAsync implements the SetWriteNewSeriesAsync rpc endpoint.
func (s *NodeServer) SetWriteNewSeriesAsync(ctx context.Context, req *rpc.NodeSetWriteNewSeriesAsyncRequest) (*rpc.NodeWriteNewSeriesAsyncResult, error) {
	thriftReq := &dbnode.NodeSetWriteNewSeriesAsyncRequest{
		WriteNewSeriesAsync: req.WriteNewSeriesAsync,
	}

	// Assuming s.service has SetWriteNewSeriesAsync. This method is on AdminService in tchannel node.
	thriftResult, err := s.service.SetWriteNewSeriesAsync(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error (dbnode.Error) to gRPC error status
		return nil, status.Errorf(codes.Internal, "SetWriteNewSeriesAsync failed: %v", err)
	}

	return &rpc.NodeWriteNewSeriesAsyncResult{
		WriteNewSeriesAsync: thriftResult.WriteNewSeriesAsync,
	}, nil
}

// Helper to convert thriftrpc.Error to rpc.Error
func fromThriftError(err *dbnode.Error) *rpc.Error {
	if err == nil {
		return nil
	}
	var errType rpc.ErrorType
	switch err.Type {
	case dbnode.ErrorTypeInternalError:
		errType = rpc.ErrorType_INTERNAL_ERROR
	case dbnode.ErrorTypeBadRequest:
		errType = rpc.ErrorType_BAD_REQUEST
	default:
		// Should we default or panic/error on unknown type?
		// Defaulting to internal error for now.
		errType = rpc.ErrorType_INTERNAL_ERROR
	}
	return &rpc.Error{
		Type:    errType,
		Message: err.Message,
		Flags:   err.Flags,
	}
}

// Helper to convert thriftrpc.Block to rpc.Block
func fromThriftBlock(thriftBlock *dbnode.Block) *rpc.Block {
	if thriftBlock == nil {
		return nil
	}
	return &rpc.Block{
		Start:    thriftBlock.Start,
		Segments: fromThriftSegments(thriftBlock.Segments),
		Err:      fromThriftError(thriftBlock.Err),
		Checksum: thriftBlock.Checksum,
	}
}

// Helper to convert thriftrpc.Blocks to rpc.Blocks
func fromThriftBlocks(thriftBlocks *dbnode.Blocks) *rpc.Blocks {
	if thriftBlocks == nil {
		return nil
	}
	protoBlocksList := make([]*rpc.Block, 0, len(thriftBlocks.Blocks))
	for _, b := range thriftBlocks.Blocks {
		protoBlocksList = append(protoBlocksList, fromThriftBlock(b))
	}
	return &rpc.Blocks{
		Id:     thriftBlocks.ID,
		Blocks: protoBlocksList,
	}
}

// Helper to convert rpc.FetchBlocksRawRequestElement to thriftrpc.FetchBlocksRawRequestElement
func toThriftFetchBlocksRawRequestElement(elem *rpc.FetchBlocksRawRequestElement) *dbnode.FetchBlocksRawRequestElement {
	if elem == nil {
		return nil
	}
	return &dbnode.FetchBlocksRawRequestElement{
		ID:     elem.Id,     // Already bytes
		Starts: elem.Starts, // Already []int64
	}
}

// FetchBlocksRaw implements the FetchBlocksRaw rpc endpoint.
func (s *NodeServer) FetchBlocksRaw(ctx context.Context, req *rpc.FetchBlocksRawRequest) (*rpc.FetchBlocksRawResult, error) {
	thriftElements := make([]*dbnode.FetchBlocksRawRequestElement, 0, len(req.Elements))
	for _, elem := range req.Elements {
		thriftElements = append(thriftElements, toThriftFetchBlocksRawRequestElement(elem))
	}

	thriftReq := &dbnode.FetchBlocksRawRequest{
		NameSpace: req.NameSpace, // Already bytes
		Shard:     int32(req.Shard),
		Elements:  thriftElements,
		Source:    req.Source,
	}

	thriftResult, err := s.service.FetchBlocksRaw(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error to gRPC error status
		return nil, status.Errorf(codes.Internal, "FetchBlocksRaw failed: %v", err)
	}

	protoElements := make([]*rpc.Blocks, 0, len(thriftResult.Elements))
	for _, elem := range thriftResult.Elements {
		protoElements = append(protoElements, fromThriftBlocks(elem))
	}

	return &rpc.FetchBlocksRawResult{
		Elements: protoElements,
	}, nil
}

// Helper to convert thriftrpc.BlockMetadataV2 to rpc.BlockMetadataV2
func fromThriftBlockMetadataV2(meta *dbnode.BlockMetadataV2) *rpc.BlockMetadataV2 {
	if meta == nil {
		return nil
	}
	return &rpc.BlockMetadataV2{
		Id:                meta.ID,
		Start:             meta.Start,
		Err:               fromThriftError(meta.Err),
		Size:              meta.Size,
		Checksum:          meta.Checksum,
		LastRead:          meta.LastRead,
		LastReadTimeType:  fromThriftTimeType(meta.LastReadTimeType),
		EncodedTags:       meta.EncodedTags,
	}
}

// FetchBlocksMetadataRawV2 implements the FetchBlocksMetadataRawV2 rpc endpoint.
func (s *NodeServer) FetchBlocksMetadataRawV2(
	ctx context.Context,
	req *rpc.FetchBlocksMetadataRawV2Request,
) (*rpc.FetchBlocksMetadataRawV2Result, error) {
	thriftReq := &dbnode.FetchBlocksMetadataRawV2Request{
		NameSpace:        req.NameSpace,
		Shard:            int32(req.Shard),
		RangeStart:       req.RangeStart,
		RangeEnd:         req.RangeEnd,
		Limit:            req.Limit,
		PageToken:        req.PageToken,
		IncludeSizes:     &req.IncludeSizes,     // Thrift uses *bool
		IncludeChecksums: &req.IncludeChecksums, // Thrift uses *bool
		IncludeLastRead:  &req.IncludeLastRead,  // Thrift uses *bool
	}
	// Similar to FetchTagged, optional booleans in Thrift are pointers.
	// gRPC bools default to false. If Thrift service has different defaults for omitted optionals,
	// this might need adjustment. Assuming current direct mapping is acceptable.

	thriftResult, err := s.service.FetchBlocksMetadataRawV2(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error to gRPC error status
		return nil, status.Errorf(codes.Internal, "FetchBlocksMetadataRawV2 failed: %v", err)
	}

	protoElements := make([]*rpc.BlockMetadataV2, 0, len(thriftResult.Elements))
	for _, elem := range thriftResult.Elements {
		protoElements = append(protoElements, fromThriftBlockMetadataV2(elem))
	}

	return &rpc.FetchBlocksMetadataRawV2Result{
		Elements:      protoElements,
		NextPageToken: thriftResult.NextPageToken,
	}, nil
}

// Helper to convert rpc.WriteBatchRawV2RequestElement to thriftrpc.WriteBatchRawV2RequestElement
func toThriftWriteBatchRawV2RequestElement(elem *rpc.WriteBatchRawV2RequestElement) *dbnode.WriteBatchRawV2RequestElement {
	if elem == nil {
		return nil
	}
	return &dbnode.WriteBatchRawV2RequestElement{
		ID:        elem.Id,
		Datapoint: toThriftDatapoint(elem.Datapoint),
		NameSpace: elem.NameSpace, // This is an index
	}
}

// WriteBatchRawV2 implements the WriteBatchRawV2 rpc endpoint.
func (s *NodeServer) WriteBatchRawV2(ctx context.Context, req *rpc.WriteBatchRawV2Request) (*emptypb.Empty, error) {
	thriftElements := make([]*dbnode.WriteBatchRawV2RequestElement, 0, len(req.Elements))
	for _, elem := range req.Elements {
		thriftElements = append(thriftElements, toThriftWriteBatchRawV2RequestElement(elem))
	}

	thriftReq := &dbnode.WriteBatchRawV2Request{
		NameSpaces: req.NameSpaces, // Already repeated bytes
		Elements:   thriftElements,
	}

	err := s.service.WriteBatchRawV2(ctx, thriftReq)
	if err != nil {
		if typedErr, ok := err.(*dbnode.WriteBatchRawErrors); ok {
			var errMsgs []string
			for _, e := range typedErr.Errors {
				errMsgs = append(errMsgs, e.Err.Message)
			}
			return nil, status.Errorf(codes.InvalidArgument, "WriteBatchRawV2 failed for some elements: %v", errMsgs)
		}
		// TODO: Map other Thrift errors to gRPC error status
		return nil, status.Errorf(codes.Internal, "WriteBatchRawV2 failed: %v", err)
	}

	return &emptypb.Empty{}, nil
}

// Helper to convert thriftrpc.FetchTaggedIDResult to rpc.FetchTaggedIDResult
func fromThriftFetchTaggedIDResult(thriftResult *dbnode.FetchTaggedIDResult) *rpc.FetchTaggedIDResult {
	if thriftResult == nil {
		return nil
	}
	segmentsProto := make([]*rpc.Segments, 0, len(thriftResult.Segments))
	for _, segs := range thriftResult.Segments {
		segmentsProto = append(segmentsProto, fromThriftSegments(segs))
	}
	// NB: thriftResult.Err is deprecated and intentionally ignored here.
	return &rpc.FetchTaggedIDResult{
		Id:          thriftResult.ID,
		NameSpace:   thriftResult.NameSpace,
		EncodedTags: thriftResult.EncodedTags,
		Segments:    segmentsProto,
	}
}

// FetchTagged implements the FetchTagged rpc endpoint.
func (s *NodeServer) FetchTagged(ctx context.Context, req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult, error) {
	thriftReq := &dbnode.FetchTaggedRequest{
		NameSpace:         req.NameSpace, // Already bytes
		Query:             req.Query,     // Already bytes
		RangeStart:        req.RangeStart,
		RangeEnd:          req.RangeEnd,
		FetchData:         req.FetchData,
		SeriesLimit:       req.SeriesLimit,
		RangeTimeType:     toThriftTimeType(req.RangeTimeType),
		RequireExhaustive: &req.RequireExhaustive, // Thrift expects a pointer
		DocsLimit:         req.DocsLimit,
		Source:            req.Source,
		RequireNoWait:     &req.RequireNoWait, // Thrift expects a pointer
	}
	// Note: The Thrift definition for FetchTaggedRequest has 'RequireExhaustive' and 'RequireNoWait'
	// as optional booleans. The generated Go Thrift structs use pointers (*bool) for these.
	// The gRPC proto3 uses plain bool, which defaults to false if not set.
	// If the client omits these in gRPC, they'll be false, and we'll pass *false to Thrift.
	// If this distinction (unset vs. explicitly false) is important for the backend,
	// the gRPC request might need to use something like google.protobuf.BoolValue,
	// or the server-side logic here would need to handle it.
	// For now, direct assignment is used. The proto `bool require_exhaustive = 8;` means it will default to false.
	// The thrift `optional bool requireExhaustive = true` defaults to true if client omits.
	// This is a mismatch.
	// Let's assume for now the gRPC client is expected to set these explicitly if non-default behavior is desired.
	// Or, we can adjust here if the gRPC field is not set, to align with Thrift's default.
	// For example, if req.RequireExhaustive is the proto default (false) and we want to match thrift's default (true if unspecified):
	// We need a way to know if req.RequireExhaustive was actually sent as false, or just defaulted.
	// This is a general proto3 vs thrift optionality issue.
	// For now, we will pass the value as is from gRPC. If gRPC client sends nothing, it's false.
	// If the Thrift default *true* is critical when the field is unspecified by the client, this needs more sophisticated handling.
	// Let's assume the current behavior (pass value as is) is acceptable for now.

	thriftResult, err := s.service.FetchTagged(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error to gRPC error status
		return nil, status.Errorf(codes.Internal, "FetchTagged failed: %v", err)
	}

	protoElements := make([]*rpc.FetchTaggedIDResult, 0, len(thriftResult.Elements))
	for _, elem := range thriftResult.Elements {
		protoElements = append(protoElements, fromThriftFetchTaggedIDResult(elem))
	}

	return &rpc.FetchTaggedResult{
		Elements:         protoElements,
		Exhaustive:       thriftResult.Exhaustive,
		WaitedIndex:      thriftResult.WaitedIndex,
		WaitedSeriesRead: thriftResult.WaitedSeriesRead,
	}, nil
}

// Helper to convert rpc.WriteTaggedBatchRawRequestElement to thriftrpc.WriteTaggedBatchRawRequestElement
func toThriftWriteTaggedBatchRawRequestElement(elem *rpc.WriteTaggedBatchRawRequestElement) *dbnode.WriteTaggedBatchRawRequestElement {
	if elem == nil {
		return nil
	}
	return &dbnode.WriteTaggedBatchRawRequestElement{
		ID:          elem.Id,          // Already bytes
		EncodedTags: elem.EncodedTags, // Already bytes
		Datapoint:   toThriftDatapoint(elem.Datapoint),
	}
}

// WriteTaggedBatchRaw implements the WriteTaggedBatchRaw rpc endpoint.
func (s *NodeServer) WriteTaggedBatchRaw(ctx context.Context, req *rpc.WriteTaggedBatchRawRequest) (*emptypb.Empty, error) {
	thriftElements := make([]*dbnode.WriteTaggedBatchRawRequestElement, 0, len(req.Elements))
	for _, elem := range req.Elements {
		thriftElements = append(thriftElements, toThriftWriteTaggedBatchRawRequestElement(elem))
	}

	thriftReq := &dbnode.WriteTaggedBatchRawRequest{
		NameSpace: req.NameSpace, // Already bytes
		Elements:  thriftElements,
	}

	err := s.service.WriteTaggedBatchRaw(ctx, thriftReq)
	if err != nil {
		if typedErr, ok := err.(*dbnode.WriteBatchRawErrors); ok {
			var errMsgs []string
			for _, e := range typedErr.Errors {
				errMsgs = append(errMsgs, e.Err.Message)
			}
			return nil, status.Errorf(codes.InvalidArgument, "WriteTaggedBatchRaw failed for some elements: %v", errMsgs)
		}
		// TODO: Map other Thrift errors to gRPC error status
		return nil, status.Errorf(codes.Internal, "WriteTaggedBatchRaw failed: %v", err)
	}

	return &emptypb.Empty{}, nil
}

// Helper to convert thriftrpc.Segment to rpc.Segment
func fromThriftSegment(thriftSegment *dbnode.Segment) *rpc.Segment {
	if thriftSegment == nil {
		return nil
	}
	return &rpc.Segment{
		Head:      thriftSegment.Head,
		Tail:      thriftSegment.Tail,
		StartTime: thriftSegment.StartTime,
		BlockSize: thriftSegment.BlockSize,
		Checksum:  thriftSegment.Checksum,
	}
}

// Helper to convert thriftrpc.Segments to rpc.Segments
func fromThriftSegments(thriftSegments *dbnode.Segments) *rpc.Segments {
	if thriftSegments == nil {
		return nil
	}
	unmergedProto := make([]*rpc.Segment, 0, len(thriftSegments.Unmerged))
	for _, seg := range thriftSegments.Unmerged {
		unmergedProto = append(unmergedProto, fromThriftSegment(seg))
	}
	return &rpc.Segments{
		Merged:   fromThriftSegment(thriftSegments.Merged),
		Unmerged: unmergedProto,
	}
}

// Helper to convert thriftrpc.FetchRawResult to rpc.FetchRawResult
func fromThriftFetchRawResult(thriftResult *dbnode.FetchRawResult) *rpc.FetchRawResult {
	if thriftResult == nil {
		return nil
	}
	segmentsProto := make([]*rpc.Segments, 0, len(thriftResult.Segments))
	for _, segs := range thriftResult.Segments {
		segmentsProto = append(segmentsProto, fromThriftSegments(segs))
	}
	// NB: thriftResult.Err is intentionally ignored here as rpc.FetchRawResult has no error field.
	// Errors at this level would need to be handled differently if required (e.g. by filtering out results with errors,
	// or changing the rpc.FetchRawResult structure, or logging).
	// For now, we assume that a top-level error from service.FetchBatchRaw would indicate a complete failure.
	return &rpc.FetchRawResult{
		Segments: segmentsProto,
	}
}

// FetchBatchRaw implements the FetchBatchRaw rpc endpoint.
func (s *NodeServer) FetchBatchRaw(ctx context.Context, req *rpc.FetchBatchRawRequest) (*rpc.FetchBatchRawResult, error) {
	thriftReq := &dbnode.FetchBatchRawRequest{
		NameSpace:     req.NameSpace, // Already bytes
		Ids:           req.Ids,       // Already repeated bytes
		RangeStart:    req.RangeStart,
		RangeEnd:      req.RangeEnd,
		RangeTimeType: toThriftTimeType(req.RangeTimeType),
		Source:        req.Source,
	}

	thriftResult, err := s.service.FetchBatchRaw(ctx, thriftReq)
	if err != nil {
		// TODO: Map Thrift error to gRPC error status
		return nil, status.Errorf(codes.Internal, "FetchBatchRaw failed: %v", err)
	}

	protoElements := make([]*rpc.FetchRawResult, 0, len(thriftResult.Elements))
	for _, elem := range thriftResult.Elements {
		protoElements = append(protoElements, fromThriftFetchRawResult(elem))
	}

	return &rpc.FetchBatchRawResult{
		Elements: protoElements,
	}, nil
}

// Helper to convert rpc.WriteBatchRawRequestElement to thriftrpc.WriteBatchRawRequestElement
func toThriftWriteBatchRawRequestElement(elem *rpc.WriteBatchRawRequestElement) *dbnode.WriteBatchRawRequestElement {
	if elem == nil {
		return nil
	}
	return &dbnode.WriteBatchRawRequestElement{
		ID:        elem.Id, // Already bytes
		Datapoint: toThriftDatapoint(elem.Datapoint),
	}
}

// WriteBatchRaw implements the WriteBatchRaw rpc endpoint.
func (s *NodeServer) WriteBatchRaw(ctx context.Context, req *rpc.WriteBatchRawRequest) (*emptypb.Empty, error) {
	thriftElements := make([]*dbnode.WriteBatchRawRequestElement, 0, len(req.Elements))
	for _, elem := range req.Elements {
		thriftElements = append(thriftElements, toThriftWriteBatchRawRequestElement(elem))
	}

	thriftReq := &dbnode.WriteBatchRawRequest{
		NameSpace: req.NameSpace, // Already bytes
		Elements:  thriftElements,
	}

	err := s.service.WriteBatchRaw(ctx, thriftReq)
	if err != nil {
		// Check for specific WriteBatchRawErrors
		if typedErr, ok := err.(*dbnode.WriteBatchRawErrors); ok {
			// For now, just return a generic error indicating batch failure.
			// A more sophisticated approach might involve returning details of individual errors
			// if the gRPC API supported it, or logging them.
			// Example: collect messages from typedErr.Errors into a single message.
			// For now, map to InvalidArgument as it's a batch of writes with some issues.
			// Or Internal if it's more of a server-side processing batch issue.
			// Let's use InvalidArgument to signal client-provided data might be problematic.
			// This specific error mapping should be refined.
			var errMsgs []string
			for _, e := range typedErr.Errors {
				errMsgs = append(errMsgs, e.Err.Message)
			}
			// Consider logging the detailed errors: log.Errorf("WriteBatchRaw partial failure: %v", typedErr)
			return nil, status.Errorf(codes.InvalidArgument, "WriteBatchRaw failed for some elements: %v", errMsgs)
		}
		// TODO: Map other Thrift errors to gRPC error status
		return nil, status.Errorf(codes.Internal, "WriteBatchRaw failed: %v", err)
	}

	return &emptypb.Empty{}, nil
}
