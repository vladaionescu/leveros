package core

import (
	grpc "github.com/leveros/grpc-go"
	"github.com/leveros/grpc-go/metadata"
	"golang.org/x/net/context"
)

// SendLeverRPC sends a Lever RPC to a certain resource within the service
// being contacted.
func SendLeverRPC(
	conn *grpc.ClientConn, ctx context.Context, leverURL *LeverURL, in *RPC,
	opts ...grpc.CallOption) (*RPCReply, error) {
	ctx = createContext(ctx, leverURL)
	return NewLeverRPCClient(conn).HandleRPC(ctx, in, opts...)
}

// SendStreamingLeverRPC sends a Lever streaming RPC to a certain resource
// within the service being contacted.
func SendStreamingLeverRPC(
	conn *grpc.ClientConn, ctx context.Context, leverURL *LeverURL,
	opts ...grpc.CallOption) (
	LeverRPC_HandleStreamingRPCClient, error) {
	ctx = createContext(ctx, leverURL)
	return NewLeverRPCClient(conn).HandleStreamingRPC(ctx, opts...)
}

func createContext(ctx context.Context, leverURL *LeverURL) context.Context {
	return metadata.NewContext(ctx, metadata.Pairs(
		"lever-url", leverURL.String(),
	))
}
