package testutil

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Lazily implemented imitation of grpc.ServerStream, useful for directly
// invoking streaming grpc endpoints when testing servers
type FakeServerStream struct {
	ctx context.Context
}

var _ grpc.ServerStream = &FakeServerStream{}

func NewFakeServerStream(ctx context.Context) *FakeServerStream {
	return &FakeServerStream{
		ctx: ctx,
	}
}

func (f *FakeServerStream) SetHeader(metadata.MD) error {
	panic("not implemented")
}

func (f *FakeServerStream) SendHeader(metadata.MD) error {
	panic("not implemented")
}

func (f *FakeServerStream) SetTrailer(metadata.MD) {
	panic("not implemented")
}

func (f *FakeServerStream) Context() context.Context {
	return f.ctx
}
func (f *FakeServerStream) SendMsg(m interface{}) error {
	panic("not implemented")
}

func (f *FakeServerStream) RecvMsg(m interface{}) error {
	panic("not implemented")
}
