package core

import (
	"fmt"

	"golang.org/x/net/context"
	grpc "github.com/leveros/grpc-go"
	"github.com/leveros/grpc-go/metadata"
)

// SendLeverRPC sends a Lever RPC to a certain resource within the service
// being contacted.
func SendLeverRPC(
	conn *grpc.ClientConn, ctx context.Context, env string, service string,
	resource string, in *RPC, opts ...grpc.CallOption) (*RPCReply, error) {
	ctx = createContext(ctx, env)
	path := fmt.Sprintf("/%s/%s/HandleRPC", service, resource)
	out := new(RPCReply)
	err := grpc.Invoke(ctx, path, in, out, conn, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SendStreamingLeverRPC sends a Lever streaming RPC to a certain resource
// within the service being contacted.
func SendStreamingLeverRPC(
	conn *grpc.ClientConn, ctx context.Context, env string, service string,
	resource string, opts ...grpc.CallOption) (
	LeverRPC_HandleStreamingRPCClient, error) {
	ctx = createContext(ctx, env)
	path := fmt.Sprintf("/%s/%s/HandleStreamingRPC", service, resource)
	stream, err := grpc.NewClientStream(
		ctx, &_LeverRPC_serviceDesc.Streams[0], conn, path, opts...)
	if err != nil {
		return nil, err
	}
	x := &leverRPCHandleStreamingRPCClient{stream}
	return x, nil
}

func createContext(ctx context.Context, env string) context.Context {
	return metadata.NewContext(ctx, metadata.Pairs(
		"host", env,
	))
}
