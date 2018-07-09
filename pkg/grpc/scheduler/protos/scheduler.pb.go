// Code generated by protoc-gen-go. DO NOT EDIT.
// source: scheduler.proto

/*
Package scheduler_protos is a generated protocol buffer package.

It is generated from these files:
	scheduler.proto

It has these top-level messages:
	AllocateNodesRequest
	AllocateNodesResponse
	DeallocateNodesRequest
	DeallocateNodesResponse
	EligibleNodesRequest
	EligibleNodesResponse
*/
package scheduler_protos

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
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

type AllocateNodesRequest struct {
	Manifest       string `protobuf:"bytes,1,opt,name=manifest" json:"manifest,omitempty"`
	NodeSelector   string `protobuf:"bytes,2,opt,name=node_selector,json=nodeSelector" json:"node_selector,omitempty"`
	NodesRequested int64  `protobuf:"varint,3,opt,name=nodes_requested,json=nodesRequested" json:"nodes_requested,omitempty"`
	Force          bool   `protobuf:"varint,4,opt,name=force" json:"force,omitempty"`
}

func (m *AllocateNodesRequest) Reset()                    { *m = AllocateNodesRequest{} }
func (m *AllocateNodesRequest) String() string            { return proto.CompactTextString(m) }
func (*AllocateNodesRequest) ProtoMessage()               {}
func (*AllocateNodesRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *AllocateNodesRequest) GetManifest() string {
	if m != nil {
		return m.Manifest
	}
	return ""
}

func (m *AllocateNodesRequest) GetNodeSelector() string {
	if m != nil {
		return m.NodeSelector
	}
	return ""
}

func (m *AllocateNodesRequest) GetNodesRequested() int64 {
	if m != nil {
		return m.NodesRequested
	}
	return 0
}

func (m *AllocateNodesRequest) GetForce() bool {
	if m != nil {
		return m.Force
	}
	return false
}

type AllocateNodesResponse struct {
	AllocatedNodes []string `protobuf:"bytes,1,rep,name=allocated_nodes,json=allocatedNodes" json:"allocated_nodes,omitempty"`
}

func (m *AllocateNodesResponse) Reset()                    { *m = AllocateNodesResponse{} }
func (m *AllocateNodesResponse) String() string            { return proto.CompactTextString(m) }
func (*AllocateNodesResponse) ProtoMessage()               {}
func (*AllocateNodesResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

func (m *AllocateNodesResponse) GetAllocatedNodes() []string {
	if m != nil {
		return m.AllocatedNodes
	}
	return nil
}

type DeallocateNodesRequest struct {
	NodesReleased []string `protobuf:"bytes,1,rep,name=nodes_released,json=nodesReleased" json:"nodes_released,omitempty"`
	NodeSelector  string   `protobuf:"bytes,2,opt,name=node_selector,json=nodeSelector" json:"node_selector,omitempty"`
}

func (m *DeallocateNodesRequest) Reset()                    { *m = DeallocateNodesRequest{} }
func (m *DeallocateNodesRequest) String() string            { return proto.CompactTextString(m) }
func (*DeallocateNodesRequest) ProtoMessage()               {}
func (*DeallocateNodesRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *DeallocateNodesRequest) GetNodesReleased() []string {
	if m != nil {
		return m.NodesReleased
	}
	return nil
}

func (m *DeallocateNodesRequest) GetNodeSelector() string {
	if m != nil {
		return m.NodeSelector
	}
	return ""
}

type DeallocateNodesResponse struct {
}

func (m *DeallocateNodesResponse) Reset()                    { *m = DeallocateNodesResponse{} }
func (m *DeallocateNodesResponse) String() string            { return proto.CompactTextString(m) }
func (*DeallocateNodesResponse) ProtoMessage()               {}
func (*DeallocateNodesResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{3} }

type EligibleNodesRequest struct {
	Manifest     string `protobuf:"bytes,1,opt,name=manifest" json:"manifest,omitempty"`
	NodeSelector string `protobuf:"bytes,2,opt,name=node_selector,json=nodeSelector" json:"node_selector,omitempty"`
}

func (m *EligibleNodesRequest) Reset()                    { *m = EligibleNodesRequest{} }
func (m *EligibleNodesRequest) String() string            { return proto.CompactTextString(m) }
func (*EligibleNodesRequest) ProtoMessage()               {}
func (*EligibleNodesRequest) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{4} }

func (m *EligibleNodesRequest) GetManifest() string {
	if m != nil {
		return m.Manifest
	}
	return ""
}

func (m *EligibleNodesRequest) GetNodeSelector() string {
	if m != nil {
		return m.NodeSelector
	}
	return ""
}

type EligibleNodesResponse struct {
	EligibleNodes []string `protobuf:"bytes,1,rep,name=eligible_nodes,json=eligibleNodes" json:"eligible_nodes,omitempty"`
}

func (m *EligibleNodesResponse) Reset()                    { *m = EligibleNodesResponse{} }
func (m *EligibleNodesResponse) String() string            { return proto.CompactTextString(m) }
func (*EligibleNodesResponse) ProtoMessage()               {}
func (*EligibleNodesResponse) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{5} }

func (m *EligibleNodesResponse) GetEligibleNodes() []string {
	if m != nil {
		return m.EligibleNodes
	}
	return nil
}

func init() {
	proto.RegisterType((*AllocateNodesRequest)(nil), "scheduler_protos.AllocateNodesRequest")
	proto.RegisterType((*AllocateNodesResponse)(nil), "scheduler_protos.AllocateNodesResponse")
	proto.RegisterType((*DeallocateNodesRequest)(nil), "scheduler_protos.DeallocateNodesRequest")
	proto.RegisterType((*DeallocateNodesResponse)(nil), "scheduler_protos.DeallocateNodesResponse")
	proto.RegisterType((*EligibleNodesRequest)(nil), "scheduler_protos.EligibleNodesRequest")
	proto.RegisterType((*EligibleNodesResponse)(nil), "scheduler_protos.EligibleNodesResponse")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for P2Scheduler service

type P2SchedulerClient interface {
	AllocateNodes(ctx context.Context, in *AllocateNodesRequest, opts ...grpc.CallOption) (*AllocateNodesResponse, error)
	DeallocateNodes(ctx context.Context, in *DeallocateNodesRequest, opts ...grpc.CallOption) (*DeallocateNodesResponse, error)
	EligibleNodes(ctx context.Context, in *EligibleNodesRequest, opts ...grpc.CallOption) (*EligibleNodesResponse, error)
}

type p2SchedulerClient struct {
	cc *grpc.ClientConn
}

func NewP2SchedulerClient(cc *grpc.ClientConn) P2SchedulerClient {
	return &p2SchedulerClient{cc}
}

func (c *p2SchedulerClient) AllocateNodes(ctx context.Context, in *AllocateNodesRequest, opts ...grpc.CallOption) (*AllocateNodesResponse, error) {
	out := new(AllocateNodesResponse)
	err := grpc.Invoke(ctx, "/scheduler_protos.P2Scheduler/AllocateNodes", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *p2SchedulerClient) DeallocateNodes(ctx context.Context, in *DeallocateNodesRequest, opts ...grpc.CallOption) (*DeallocateNodesResponse, error) {
	out := new(DeallocateNodesResponse)
	err := grpc.Invoke(ctx, "/scheduler_protos.P2Scheduler/DeallocateNodes", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *p2SchedulerClient) EligibleNodes(ctx context.Context, in *EligibleNodesRequest, opts ...grpc.CallOption) (*EligibleNodesResponse, error) {
	out := new(EligibleNodesResponse)
	err := grpc.Invoke(ctx, "/scheduler_protos.P2Scheduler/EligibleNodes", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for P2Scheduler service

type P2SchedulerServer interface {
	AllocateNodes(context.Context, *AllocateNodesRequest) (*AllocateNodesResponse, error)
	DeallocateNodes(context.Context, *DeallocateNodesRequest) (*DeallocateNodesResponse, error)
	EligibleNodes(context.Context, *EligibleNodesRequest) (*EligibleNodesResponse, error)
}

func RegisterP2SchedulerServer(s *grpc.Server, srv P2SchedulerServer) {
	s.RegisterService(&_P2Scheduler_serviceDesc, srv)
}

func _P2Scheduler_AllocateNodes_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AllocateNodesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2SchedulerServer).AllocateNodes(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/scheduler_protos.P2Scheduler/AllocateNodes",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2SchedulerServer).AllocateNodes(ctx, req.(*AllocateNodesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _P2Scheduler_DeallocateNodes_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeallocateNodesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2SchedulerServer).DeallocateNodes(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/scheduler_protos.P2Scheduler/DeallocateNodes",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2SchedulerServer).DeallocateNodes(ctx, req.(*DeallocateNodesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _P2Scheduler_EligibleNodes_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EligibleNodesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(P2SchedulerServer).EligibleNodes(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/scheduler_protos.P2Scheduler/EligibleNodes",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(P2SchedulerServer).EligibleNodes(ctx, req.(*EligibleNodesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _P2Scheduler_serviceDesc = grpc.ServiceDesc{
	ServiceName: "scheduler_protos.P2Scheduler",
	HandlerType: (*P2SchedulerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AllocateNodes",
			Handler:    _P2Scheduler_AllocateNodes_Handler,
		},
		{
			MethodName: "DeallocateNodes",
			Handler:    _P2Scheduler_DeallocateNodes_Handler,
		},
		{
			MethodName: "EligibleNodes",
			Handler:    _P2Scheduler_EligibleNodes_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "scheduler.proto",
}

func init() { proto.RegisterFile("scheduler.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 336 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xac, 0x92, 0xc1, 0x4e, 0xf2, 0x40,
	0x10, 0xc7, 0x59, 0xf8, 0x3e, 0x03, 0xa3, 0x2d, 0x66, 0x03, 0x5a, 0x39, 0x35, 0x6b, 0x90, 0x7a,
	0xe1, 0x80, 0x77, 0xa3, 0x89, 0x5e, 0x8d, 0x29, 0x07, 0x8f, 0x4d, 0xdb, 0x1d, 0xa4, 0xc9, 0xda,
	0xc5, 0x6e, 0x79, 0x15, 0x5f, 0xc7, 0x57, 0x33, 0x6d, 0x97, 0x86, 0x96, 0x4d, 0xe4, 0xe0, 0x71,
	0xfe, 0x33, 0xfb, 0x9f, 0x99, 0xdf, 0x0e, 0x0c, 0x55, 0xbc, 0x46, 0xbe, 0x15, 0x98, 0xcd, 0x37,
	0x99, 0xcc, 0x25, 0x3d, 0xaf, 0x85, 0xa0, 0x14, 0x14, 0xfb, 0x22, 0x30, 0x7a, 0x14, 0x42, 0xc6,
	0x61, 0x8e, 0x2f, 0x92, 0xa3, 0xf2, 0xf1, 0x73, 0x8b, 0x2a, 0xa7, 0x13, 0xe8, 0x7f, 0x84, 0x69,
	0xb2, 0x42, 0x95, 0x3b, 0xc4, 0x25, 0xde, 0xc0, 0xaf, 0x63, 0x7a, 0x0d, 0x56, 0x2a, 0x39, 0x06,
	0x0a, 0x05, 0xc6, 0xb9, 0xcc, 0x9c, 0x6e, 0x59, 0x70, 0x56, 0x88, 0x4b, 0xad, 0xd1, 0x19, 0x0c,
	0x8b, 0x58, 0x05, 0x59, 0xe5, 0x88, 0xdc, 0xe9, 0xb9, 0xc4, 0xeb, 0xf9, 0x76, 0xba, 0xd7, 0x07,
	0x39, 0x1d, 0xc1, 0xff, 0x95, 0xcc, 0x62, 0x74, 0xfe, 0xb9, 0xc4, 0xeb, 0xfb, 0x55, 0xc0, 0x1e,
	0x60, 0xdc, 0x9a, 0x4b, 0x6d, 0x64, 0xaa, 0xb0, 0xf0, 0x0d, 0x75, 0x82, 0x07, 0xa5, 0x95, 0x43,
	0xdc, 0x9e, 0x37, 0xf0, 0xed, 0x5a, 0x2e, 0x1f, 0x30, 0x0e, 0x17, 0x4f, 0x18, 0x9a, 0x76, 0x9b,
	0x82, 0xbd, 0x1b, 0x4d, 0x60, 0xa8, 0x90, 0x6b, 0x07, 0x4b, 0x4f, 0x56, 0x89, 0x47, 0xad, 0xc9,
	0xae, 0xe0, 0xf2, 0xa0, 0x4b, 0x35, 0x29, 0x7b, 0x83, 0xd1, 0xb3, 0x48, 0xde, 0x93, 0x48, 0xfc,
	0x2d, 0x5a, 0x76, 0x0f, 0xe3, 0x96, 0xb1, 0x66, 0x33, 0x05, 0x1b, 0x75, 0xa2, 0x81, 0xc6, 0xc2,
	0xfd, 0xf2, 0xc5, 0x77, 0x17, 0x4e, 0x5f, 0x17, 0xcb, 0xdd, 0x2d, 0xd0, 0x08, 0xac, 0x06, 0x6b,
	0x7a, 0x33, 0x6f, 0x1f, 0xca, 0xdc, 0x74, 0x24, 0x93, 0xd9, 0xaf, 0x75, 0x1a, 0x45, 0x87, 0xae,
	0x61, 0xd8, 0xe2, 0x44, 0xbd, 0xc3, 0xd7, 0xe6, 0x0f, 0x9b, 0xdc, 0x1e, 0x51, 0x59, 0x77, 0x8a,
	0xc0, 0x6a, 0xd0, 0x31, 0x6d, 0x63, 0xfa, 0x17, 0xd3, 0x36, 0x46, 0xcc, 0xac, 0x13, 0x9d, 0x94,
	0xf9, 0xbb, 0x9f, 0x00, 0x00, 0x00, 0xff, 0xff, 0x57, 0xa1, 0x16, 0xbc, 0x62, 0x03, 0x00, 0x00,
}
