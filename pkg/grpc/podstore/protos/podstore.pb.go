// Code generated by protoc-gen-go.
// source: pkg/grpc/podstore/protos/podstore.proto
// DO NOT EDIT!

/*
Package podstore is a generated protocol buffer package.

It is generated from these files:
	pkg/grpc/podstore/protos/podstore.proto

It has these top-level messages:
	SchedulePodRequest
	SchedulePodResponse
	WatchPodStatusRequest
	PodStatusResponse
	ProcessStatus
	ExitStatus
	UnschedulePodRequest
	UnschedulePodResponse
	ListPodStatusRequest
	ListPodStatusResponse
	DeletePodStatusRequest
	DeletePodStatusResponse
	MarkPodFailedRequest
	MarkPodFailedResponse
*/
package podstore

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	"context"

	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type SchedulePodRequest struct {
	Manifest string `protobuf:"bytes,1,opt,name=manifest" json:"manifest,omitempty"`
	NodeName string `protobuf:"bytes,2,opt,name=node_name,json=nodeName" json:"node_name,omitempty"`
}

func (m *SchedulePodRequest) Reset()                    { *m = SchedulePodRequest{} }
func (m *SchedulePodRequest) String() string            { return proto.CompactTextString(m) }
func (*SchedulePodRequest) ProtoMessage()               {}
func (*SchedulePodRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *SchedulePodRequest) GetManifest() string {
	if m != nil {
		return m.Manifest
	}
	return ""
}

func (m *SchedulePodRequest) GetNodeName() string {
	if m != nil {
		return m.NodeName
	}
	return ""
}

type SchedulePodResponse struct {
	PodUniqueKey string `protobuf:"bytes,1,opt,name=pod_unique_key,json=podUniqueKey" json:"pod_unique_key,omitempty"`
}

func (m *SchedulePodResponse) Reset()                    { *m = SchedulePodResponse{} }
func (m *SchedulePodResponse) String() string            { return proto.CompactTextString(m) }
func (*SchedulePodResponse) ProtoMessage()               {}
func (*SchedulePodResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *SchedulePodResponse) GetPodUniqueKey() string {
	if m != nil {
		return m.PodUniqueKey
	}
	return ""
}

type WatchPodStatusRequest struct {
	PodUniqueKey    string `protobuf:"bytes,1,opt,name=pod_unique_key,json=podUniqueKey" json:"pod_unique_key,omitempty"`
	StatusNamespace string `protobuf:"bytes,3,opt,name=status_namespace,json=statusNamespace" json:"status_namespace,omitempty"`
	WaitForExists   bool   `protobuf:"varint,4,opt,name=wait_for_exists,json=waitForExists" json:"wait_for_exists,omitempty"`
}

func (m *WatchPodStatusRequest) Reset()                    { *m = WatchPodStatusRequest{} }
func (m *WatchPodStatusRequest) String() string            { return proto.CompactTextString(m) }
func (*WatchPodStatusRequest) ProtoMessage()               {}
func (*WatchPodStatusRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *WatchPodStatusRequest) GetPodUniqueKey() string {
	if m != nil {
		return m.PodUniqueKey
	}
	return ""
}

func (m *WatchPodStatusRequest) GetStatusNamespace() string {
	if m != nil {
		return m.StatusNamespace
	}
	return ""
}

func (m *WatchPodStatusRequest) GetWaitForExists() bool {
	if m != nil {
		return m.WaitForExists
	}
	return false
}

type PodStatusResponse struct {
	Manifest        string           `protobuf:"bytes,1,opt,name=manifest" json:"manifest,omitempty"`
	PodState        string           `protobuf:"bytes,2,opt,name=pod_state,json=podState" json:"pod_state,omitempty"`
	ProcessStatuses []*ProcessStatus `protobuf:"bytes,3,rep,name=process_statuses,json=processStatuses" json:"process_statuses,omitempty"`
}

func (m *PodStatusResponse) Reset()                    { *m = PodStatusResponse{} }
func (m *PodStatusResponse) String() string            { return proto.CompactTextString(m) }
func (*PodStatusResponse) ProtoMessage()               {}
func (*PodStatusResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

func (m *PodStatusResponse) GetManifest() string {
	if m != nil {
		return m.Manifest
	}
	return ""
}

func (m *PodStatusResponse) GetPodState() string {
	if m != nil {
		return m.PodState
	}
	return ""
}

func (m *PodStatusResponse) GetProcessStatuses() []*ProcessStatus {
	if m != nil {
		return m.ProcessStatuses
	}
	return nil
}

type ProcessStatus struct {
	LaunchableId string      `protobuf:"bytes,1,opt,name=launchable_id,json=launchableId" json:"launchable_id,omitempty"`
	EntryPoint   string      `protobuf:"bytes,2,opt,name=entry_point,json=entryPoint" json:"entry_point,omitempty"`
	LastExit     *ExitStatus `protobuf:"bytes,3,opt,name=last_exit,json=lastExit" json:"last_exit,omitempty"`
}

func (m *ProcessStatus) Reset()                    { *m = ProcessStatus{} }
func (m *ProcessStatus) String() string            { return proto.CompactTextString(m) }
func (*ProcessStatus) ProtoMessage()               {}
func (*ProcessStatus) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

func (m *ProcessStatus) GetLaunchableId() string {
	if m != nil {
		return m.LaunchableId
	}
	return ""
}

func (m *ProcessStatus) GetEntryPoint() string {
	if m != nil {
		return m.EntryPoint
	}
	return ""
}

func (m *ProcessStatus) GetLastExit() *ExitStatus {
	if m != nil {
		return m.LastExit
	}
	return nil
}

type ExitStatus struct {
	ExitTime   int64 `protobuf:"varint,1,opt,name=exit_time,json=exitTime" json:"exit_time,omitempty"`
	ExitCode   int64 `protobuf:"varint,2,opt,name=exit_code,json=exitCode" json:"exit_code,omitempty"`
	ExitStatus int64 `protobuf:"varint,3,opt,name=exit_status,json=exitStatus" json:"exit_status,omitempty"`
}

func (m *ExitStatus) Reset()                    { *m = ExitStatus{} }
func (m *ExitStatus) String() string            { return proto.CompactTextString(m) }
func (*ExitStatus) ProtoMessage()               {}
func (*ExitStatus) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

func (m *ExitStatus) GetExitTime() int64 {
	if m != nil {
		return m.ExitTime
	}
	return 0
}

func (m *ExitStatus) GetExitCode() int64 {
	if m != nil {
		return m.ExitCode
	}
	return 0
}

func (m *ExitStatus) GetExitStatus() int64 {
	if m != nil {
		return m.ExitStatus
	}
	return 0
}

type UnschedulePodRequest struct {
	PodUniqueKey string `protobuf:"bytes,1,opt,name=pod_unique_key,json=podUniqueKey" json:"pod_unique_key,omitempty"`
}

func (m *UnschedulePodRequest) Reset()                    { *m = UnschedulePodRequest{} }
func (m *UnschedulePodRequest) String() string            { return proto.CompactTextString(m) }
func (*UnschedulePodRequest) ProtoMessage()               {}
func (*UnschedulePodRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{6} }

func (m *UnschedulePodRequest) GetPodUniqueKey() string {
	if m != nil {
		return m.PodUniqueKey
	}
	return ""
}

type UnschedulePodResponse struct {
}

func (m *UnschedulePodResponse) Reset()                    { *m = UnschedulePodResponse{} }
func (m *UnschedulePodResponse) String() string            { return proto.CompactTextString(m) }
func (*UnschedulePodResponse) ProtoMessage()               {}
func (*UnschedulePodResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{7} }

type ListPodStatusRequest struct {
	StatusNamespace string `protobuf:"bytes,1,opt,name=status_namespace,json=statusNamespace" json:"status_namespace,omitempty"`
}

func (m *ListPodStatusRequest) Reset()                    { *m = ListPodStatusRequest{} }
func (m *ListPodStatusRequest) String() string            { return proto.CompactTextString(m) }
func (*ListPodStatusRequest) ProtoMessage()               {}
func (*ListPodStatusRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{8} }

func (m *ListPodStatusRequest) GetStatusNamespace() string {
	if m != nil {
		return m.StatusNamespace
	}
	return ""
}

type ListPodStatusResponse struct {
	PodStatuses map[string]*PodStatusResponse `protobuf:"bytes,1,rep,name=pod_statuses,json=podStatuses" json:"pod_statuses,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"bytes,2,opt,name=value"`
}

func (m *ListPodStatusResponse) Reset()                    { *m = ListPodStatusResponse{} }
func (m *ListPodStatusResponse) String() string            { return proto.CompactTextString(m) }
func (*ListPodStatusResponse) ProtoMessage()               {}
func (*ListPodStatusResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{9} }

func (m *ListPodStatusResponse) GetPodStatuses() map[string]*PodStatusResponse {
	if m != nil {
		return m.PodStatuses
	}
	return nil
}

type DeletePodStatusRequest struct {
	PodUniqueKey string `protobuf:"bytes,1,opt,name=pod_unique_key,json=podUniqueKey" json:"pod_unique_key,omitempty"`
}

func (m *DeletePodStatusRequest) Reset()                    { *m = DeletePodStatusRequest{} }
func (m *DeletePodStatusRequest) String() string            { return proto.CompactTextString(m) }
func (*DeletePodStatusRequest) ProtoMessage()               {}
func (*DeletePodStatusRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{10} }

func (m *DeletePodStatusRequest) GetPodUniqueKey() string {
	if m != nil {
		return m.PodUniqueKey
	}
	return ""
}

type DeletePodStatusResponse struct {
}

func (m *DeletePodStatusResponse) Reset()                    { *m = DeletePodStatusResponse{} }
func (m *DeletePodStatusResponse) String() string            { return proto.CompactTextString(m) }
func (*DeletePodStatusResponse) ProtoMessage()               {}
func (*DeletePodStatusResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{11} }

type MarkPodFailedRequest struct {
	PodUniqueKey string `protobuf:"bytes,1,opt,name=pod_unique_key,json=podUniqueKey" json:"pod_unique_key,omitempty"`
}

func (m *MarkPodFailedRequest) Reset()                    { *m = MarkPodFailedRequest{} }
func (m *MarkPodFailedRequest) String() string            { return proto.CompactTextString(m) }
func (*MarkPodFailedRequest) ProtoMessage()               {}
func (*MarkPodFailedRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{12} }

func (m *MarkPodFailedRequest) GetPodUniqueKey() string {
	if m != nil {
		return m.PodUniqueKey
	}
	return ""
}

type MarkPodFailedResponse struct {
}

func (m *MarkPodFailedResponse) Reset()                    { *m = MarkPodFailedResponse{} }
func (m *MarkPodFailedResponse) String() string            { return proto.CompactTextString(m) }
func (*MarkPodFailedResponse) ProtoMessage()               {}
func (*MarkPodFailedResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{13} }

func init() {
	proto.RegisterType((*SchedulePodRequest)(nil), "podstore.SchedulePodRequest")
	proto.RegisterType((*SchedulePodResponse)(nil), "podstore.SchedulePodResponse")
	proto.RegisterType((*WatchPodStatusRequest)(nil), "podstore.WatchPodStatusRequest")
	proto.RegisterType((*PodStatusResponse)(nil), "podstore.PodStatusResponse")
	proto.RegisterType((*ProcessStatus)(nil), "podstore.ProcessStatus")
	proto.RegisterType((*ExitStatus)(nil), "podstore.ExitStatus")
	proto.RegisterType((*UnschedulePodRequest)(nil), "podstore.UnschedulePodRequest")
	proto.RegisterType((*UnschedulePodResponse)(nil), "podstore.UnschedulePodResponse")
	proto.RegisterType((*ListPodStatusRequest)(nil), "podstore.ListPodStatusRequest")
	proto.RegisterType((*ListPodStatusResponse)(nil), "podstore.ListPodStatusResponse")
	proto.RegisterType((*DeletePodStatusRequest)(nil), "podstore.DeletePodStatusRequest")
	proto.RegisterType((*DeletePodStatusResponse)(nil), "podstore.DeletePodStatusResponse")
	proto.RegisterType((*MarkPodFailedRequest)(nil), "podstore.MarkPodFailedRequest")
	proto.RegisterType((*MarkPodFailedResponse)(nil), "podstore.MarkPodFailedResponse")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for P2PodStore service

type P2PodStoreClient interface {
	// Schedules a uuid pod on a host
	SchedulePod(ctx context.Context, in *SchedulePodRequest, opts ...grpc.CallOption) (*SchedulePodResponse, error)
	WatchPodStatus(ctx context.Context, in *WatchPodStatusRequest, opts ...grpc.CallOption) (P2PodStore_WatchPodStatusClient, error)
	UnschedulePod(ctx context.Context, in *UnschedulePodRequest, opts ...grpc.CallOption) (*UnschedulePodResponse, error)
	ListPodStatus(ctx context.Context, in *ListPodStatusRequest, opts ...grpc.CallOption) (*ListPodStatusResponse, error)
	DeletePodStatus(ctx context.Context, in *DeletePodStatusRequest, opts ...grpc.CallOption) (*DeletePodStatusResponse, error)
	MarkPodFailed(ctx context.Context, in *MarkPodFailedRequest, opts ...grpc.CallOption) (*MarkPodFailedResponse, error)
}

type p2PodStoreClient struct {
	cc *grpc.ClientConn
}

func NewP2PodStoreClient(cc *grpc.ClientConn) P2PodStoreClient {
	return &p2PodStoreClient{cc}
}

func (c *p2PodStoreClient) SchedulePod(ctx context.Context, in *SchedulePodRequest, opts ...grpc.CallOption) (*SchedulePodResponse, error) {
	out := new(SchedulePodResponse)
	err := grpc.Invoke(ctx, "/podstore.P2PodStore/SchedulePod", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *p2PodStoreClient) WatchPodStatus(ctx context.Context, in *WatchPodStatusRequest, opts ...grpc.CallOption) (P2PodStore_WatchPodStatusClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_P2PodStore_serviceDesc.Streams[0], c.cc, "/podstore.P2PodStore/WatchPodStatus", opts...)
	if err != nil {
		return nil, err
	}
	x := &p2PodStoreWatchPodStatusClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type P2PodStore_WatchPodStatusClient interface {
	Recv() (*PodStatusResponse, error)
	grpc.ClientStream
}

type p2PodStoreWatchPodStatusClient struct {
	grpc.ClientStream
}

func (x *p2PodStoreWatchPodStatusClient) Recv() (*PodStatusResponse, error) {
	m := new(PodStatusResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *p2PodStoreClient) UnschedulePod(ctx context.Context, in *UnschedulePodRequest, opts ...grpc.CallOption) (*UnschedulePodResponse, error) {
	out := new(UnschedulePodResponse)
	err := grpc.Invoke(ctx, "/podstore.P2PodStore/UnschedulePod", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *p2PodStoreClient) ListPodStatus(ctx context.Context, in *ListPodStatusRequest, opts ...grpc.CallOption) (*ListPodStatusResponse, error) {
	out := new(ListPodStatusResponse)
	err := grpc.Invoke(ctx, "/podstore.P2PodStore/ListPodStatus", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *p2PodStoreClient) DeletePodStatus(ctx context.Context, in *DeletePodStatusRequest, opts ...grpc.CallOption) (*DeletePodStatusResponse, error) {
	out := new(DeletePodStatusResponse)
	err := grpc.Invoke(ctx, "/podstore.P2PodStore/DeletePodStatus", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *p2PodStoreClient) MarkPodFailed(ctx context.Context, in *MarkPodFailedRequest, opts ...grpc.CallOption) (*MarkPodFailedResponse, error) {
	out := new(MarkPodFailedResponse)
	err := grpc.Invoke(ctx, "/podstore.P2PodStore/MarkPodFailed", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for P2PodStore service

type P2PodStoreServer interface {
	// Schedules a uuid pod on a host
	SchedulePod(context.Context, *SchedulePodRequest) (*SchedulePodResponse, error)
	WatchPodStatus(*WatchPodStatusRequest, P2PodStore_WatchPodStatusServer) error
	UnschedulePod(context.Context, *UnschedulePodRequest) (*UnschedulePodResponse, error)
	ListPodStatus(context.Context, *ListPodStatusRequest) (*ListPodStatusResponse, error)
	DeletePodStatus(context.Context, *DeletePodStatusRequest) (*DeletePodStatusResponse, error)
	MarkPodFailed(context.Context, *MarkPodFailedRequest) (*MarkPodFailedResponse, error)
}

func RegisterP2PodStoreServer(s *grpc.Server, srv P2PodStoreServer) {
	s.RegisterService(&_P2PodStore_serviceDesc, srv)
}

func _P2PodStore_SchedulePod_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SchedulePodRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2PodStoreServer).SchedulePod(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/podstore.P2PodStore/SchedulePod",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2PodStoreServer).SchedulePod(ctx, req.(*SchedulePodRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _P2PodStore_WatchPodStatus_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(WatchPodStatusRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(P2PodStoreServer).WatchPodStatus(m, &p2PodStoreWatchPodStatusServer{stream})
}

type P2PodStore_WatchPodStatusServer interface {
	Send(*PodStatusResponse) error
	grpc.ServerStream
}

type p2PodStoreWatchPodStatusServer struct {
	grpc.ServerStream
}

func (x *p2PodStoreWatchPodStatusServer) Send(m *PodStatusResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _P2PodStore_UnschedulePod_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UnschedulePodRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2PodStoreServer).UnschedulePod(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/podstore.P2PodStore/UnschedulePod",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2PodStoreServer).UnschedulePod(ctx, req.(*UnschedulePodRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _P2PodStore_ListPodStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListPodStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2PodStoreServer).ListPodStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/podstore.P2PodStore/ListPodStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2PodStoreServer).ListPodStatus(ctx, req.(*ListPodStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _P2PodStore_DeletePodStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeletePodStatusRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2PodStoreServer).DeletePodStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/podstore.P2PodStore/DeletePodStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2PodStoreServer).DeletePodStatus(ctx, req.(*DeletePodStatusRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _P2PodStore_MarkPodFailed_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MarkPodFailedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2PodStoreServer).MarkPodFailed(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/podstore.P2PodStore/MarkPodFailed",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2PodStoreServer).MarkPodFailed(ctx, req.(*MarkPodFailedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _P2PodStore_serviceDesc = grpc.ServiceDesc{
	ServiceName: "podstore.P2PodStore",
	HandlerType: (*P2PodStoreServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SchedulePod",
			Handler:    _P2PodStore_SchedulePod_Handler,
		},
		{
			MethodName: "UnschedulePod",
			Handler:    _P2PodStore_UnschedulePod_Handler,
		},
		{
			MethodName: "ListPodStatus",
			Handler:    _P2PodStore_ListPodStatus_Handler,
		},
		{
			MethodName: "DeletePodStatus",
			Handler:    _P2PodStore_DeletePodStatus_Handler,
		},
		{
			MethodName: "MarkPodFailed",
			Handler:    _P2PodStore_MarkPodFailed_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "WatchPodStatus",
			Handler:       _P2PodStore_WatchPodStatus_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "pkg/grpc/podstore/protos/podstore.proto",
}

func init() { proto.RegisterFile("pkg/grpc/podstore/protos/podstore.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 675 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88, 0x02, 0xff, 0x9c, 0x55, 0x4d, 0x6f, 0xd3, 0x4a,
	0x14, 0x8d, 0x9b, 0xbe, 0xa7, 0xf4, 0xa6, 0x69, 0xf2, 0xe6, 0xa5, 0x34, 0xa4, 0x40, 0xc3, 0x80,
	0xa0, 0x6c, 0xfa, 0x11, 0x36, 0x08, 0x2a, 0x24, 0x3e, 0x5a, 0x09, 0xd1, 0x56, 0x91, 0x4b, 0x05,
	0x12, 0x0b, 0x6b, 0x6a, 0xdf, 0xb6, 0x56, 0x1d, 0xcf, 0xd4, 0x33, 0x86, 0x76, 0xcf, 0x82, 0x25,
	0x7f, 0x8b, 0x7f, 0x85, 0x66, 0xec, 0xd8, 0x4e, 0xe2, 0x96, 0xc2, 0x2e, 0xf7, 0xdc, 0x8f, 0x39,
	0x73, 0xee, 0xc9, 0x18, 0x1e, 0x8b, 0xb3, 0x93, 0xf5, 0x93, 0x48, 0xb8, 0xeb, 0x82, 0x7b, 0x52,
	0xf1, 0x08, 0xd7, 0x45, 0xc4, 0x15, 0x97, 0x59, 0xbc, 0x66, 0x62, 0x52, 0x1b, 0xc5, 0x74, 0x0f,
	0xc8, 0x81, 0x7b, 0x8a, 0x5e, 0x1c, 0xe0, 0x80, 0x7b, 0x36, 0x9e, 0xc7, 0x28, 0x15, 0xe9, 0x42,
	0x6d, 0xc8, 0x42, 0xff, 0x18, 0xa5, 0xea, 0x58, 0x3d, 0x6b, 0x75, 0xce, 0xce, 0x62, 0xb2, 0x0c,
	0x73, 0x21, 0xf7, 0xd0, 0x09, 0xd9, 0x10, 0x3b, 0x33, 0x49, 0x52, 0x03, 0xfb, 0x6c, 0x88, 0xf4,
	0x05, 0xfc, 0x3f, 0x36, 0x4e, 0x0a, 0x1e, 0x4a, 0x24, 0x0f, 0x61, 0x41, 0x70, 0xcf, 0x89, 0x43,
	0xff, 0x3c, 0x46, 0xe7, 0x0c, 0x2f, 0xd3, 0xa9, 0xf3, 0x82, 0x7b, 0x87, 0x06, 0x7c, 0x8f, 0x97,
	0xf4, 0x87, 0x05, 0x8b, 0x1f, 0x99, 0x72, 0x4f, 0x07, 0xdc, 0x3b, 0x50, 0x4c, 0xc5, 0x72, 0xc4,
	0xe7, 0x46, 0xfd, 0xe4, 0x09, 0xb4, 0xa4, 0x69, 0x33, 0xdc, 0xa4, 0x60, 0x2e, 0x76, 0xaa, 0xa6,
	0xae, 0x99, 0xe0, 0xfb, 0x23, 0x98, 0x3c, 0x82, 0xe6, 0x57, 0xe6, 0x2b, 0xe7, 0x98, 0x47, 0x0e,
	0x5e, 0xf8, 0x52, 0xc9, 0xce, 0x6c, 0xcf, 0x5a, 0xad, 0xd9, 0x0d, 0x0d, 0xef, 0xf0, 0x68, 0xdb,
	0x80, 0x9a, 0xd2, 0x7f, 0x05, 0x36, 0xe9, 0x75, 0x7e, 0x23, 0x8f, 0xa6, 0xaa, 0x0f, 0xcc, 0xe4,
	0x11, 0xc9, 0x04, 0x24, 0xaf, 0xa1, 0x25, 0x22, 0xee, 0xa2, 0x94, 0x4e, 0xc2, 0x08, 0x65, 0xa7,
	0xda, 0xab, 0xae, 0xd6, 0xfb, 0x4b, 0x6b, 0xd9, 0x8a, 0x06, 0x49, 0x45, 0x7a, 0x66, 0x53, 0x14,
	0x43, 0x94, 0xf4, 0xbb, 0x05, 0x8d, 0xb1, 0x12, 0xf2, 0x00, 0x1a, 0x01, 0x8b, 0x43, 0xf7, 0x94,
	0x1d, 0x05, 0xe8, 0xf8, 0xde, 0x48, 0x9c, 0x1c, 0x7c, 0xe7, 0x91, 0x15, 0xa8, 0x63, 0xa8, 0xa2,
	0x4b, 0x47, 0x70, 0x3f, 0x54, 0x29, 0x33, 0x30, 0xd0, 0x40, 0x23, 0x64, 0x13, 0xe6, 0x02, 0x26,
	0x95, 0x96, 0x43, 0x19, 0xd9, 0xea, 0xfd, 0x76, 0x4e, 0x6a, 0xfb, 0xc2, 0x57, 0x29, 0xa3, 0x9a,
	0x2e, 0xd3, 0x31, 0x3d, 0x01, 0xc8, 0x71, 0x7d, 0x73, 0xdd, 0xeb, 0x28, 0x7f, 0x88, 0x86, 0x42,
	0xd5, 0xae, 0x69, 0xe0, 0x83, 0x3f, 0xc4, 0x2c, 0xe9, 0x72, 0x2f, 0x91, 0x25, 0x4d, 0xbe, 0xe1,
	0x1e, 0x1a, 0x6e, 0x3a, 0x99, 0x68, 0x62, 0x0e, 0xaf, 0xda, 0x80, 0xd9, 0x68, 0xba, 0x05, 0xed,
	0xc3, 0x50, 0x4e, 0xfb, 0xf4, 0x66, 0xbe, 0x5a, 0x82, 0xc5, 0x89, 0xee, 0x64, 0x8f, 0xf4, 0x15,
	0xb4, 0x77, 0x7d, 0xa9, 0xa6, 0xec, 0x56, 0x66, 0x24, 0xab, 0xd4, 0x48, 0xf4, 0xa7, 0x05, 0x8b,
	0x13, 0x33, 0x52, 0x93, 0x1c, 0xc0, 0xfc, 0xc8, 0x08, 0x66, 0xcf, 0x96, 0xd9, 0xf3, 0x46, 0x2e,
	0x69, 0x69, 0xdb, 0x5a, 0x86, 0xa0, 0xdc, 0xd6, 0xcb, 0xb1, 0xeb, 0x22, 0x47, 0xba, 0x9f, 0xa1,
	0x35, 0x59, 0x40, 0x5a, 0x50, 0xcd, 0x6f, 0xae, 0x7f, 0x92, 0x4d, 0xf8, 0xe7, 0x0b, 0x0b, 0xe2,
	0x44, 0xe8, 0x7a, 0x7f, 0xb9, 0xe0, 0xad, 0xc9, 0xf3, 0xec, 0xa4, 0xf2, 0xf9, 0xcc, 0x33, 0x8b,
	0xbe, 0x84, 0x5b, 0x6f, 0x31, 0x40, 0x85, 0x7f, 0xf7, 0xff, 0xa3, 0xb7, 0x61, 0x69, 0xaa, 0x3f,
	0x55, 0x7a, 0x0b, 0xda, 0x7b, 0x2c, 0x3a, 0x1b, 0x70, 0x6f, 0x87, 0xf9, 0x01, 0xfe, 0xf9, 0x02,
	0x27, 0xba, 0x93, 0xb1, 0xfd, 0x6f, 0xb3, 0x00, 0x83, 0xbe, 0x39, 0x8e, 0x47, 0x48, 0x76, 0xa1,
	0x5e, 0x78, 0x7d, 0xc8, 0x9d, 0xfc, 0xde, 0xd3, 0x6f, 0x5c, 0xf7, 0xee, 0x15, 0xd9, 0x94, 0x71,
	0x85, 0xd8, 0xb0, 0x30, 0xfe, 0x1a, 0x91, 0x95, 0xbc, 0xa5, 0xf4, 0x9d, 0xea, 0x5e, 0xa7, 0x34,
	0xad, 0x6c, 0x58, 0xc4, 0x86, 0xc6, 0x98, 0x15, 0xc9, 0xbd, 0xbc, 0xa3, 0xcc, 0xe1, 0xdd, 0x95,
	0x2b, 0xf3, 0x05, 0x9e, 0x8d, 0x31, 0x2b, 0x15, 0x67, 0x96, 0xd9, 0xbb, 0x38, 0xb3, 0xd4, 0x83,
	0xb4, 0x42, 0x3e, 0x41, 0x73, 0x62, 0x95, 0xa4, 0x97, 0x77, 0x95, 0xbb, 0xa4, 0x7b, 0xff, 0x9a,
	0x8a, 0x22, 0xdb, 0xb1, 0x5d, 0x16, 0xd9, 0x96, 0x59, 0xa4, 0xc8, 0xb6, 0xd4, 0x04, 0xb4, 0x72,
	0xf4, 0xaf, 0xf9, 0xaa, 0x3d, 0xfd, 0x15, 0x00, 0x00, 0xff, 0xff, 0xde, 0x69, 0x60, 0xc2, 0x00,
	0x07, 0x00, 0x00,
}
