package api

import (
	"fmt"

	"github.com/leveros/leveros/core"
	"golang.org/x/net/context"
)

// Stream is either the first parameter of a Lever channel method (server) or
// the return value of a streaming RPC (client). It can be used to
// send and receive messages related to a streaming RPC.
type Stream interface {
	// Send sends a message to the other end of the channel.
	Send(msg interface{}) error
	// Receive receives the next message from the the channel and populates
	// msgObj with it. If the other end has closed then this returns io.EOF
	// (and doesn't populate msgObj).
	Receive(msgObj interface{}) error
	// Close closes this end of the channel. This means that writing is no
	// longer possible, though receiving can continue normally until the other
	// end closes the channel itself.
	Close() error
	// Context returns the context associated with the channel.
	Context() context.Context
}

// ClientStream is the client Stream object that is returned when invoking
// a streaming RPC.
type ClientStream struct {
	grpcStream core.LeverRPC_HandleStreamingRPCClient
}

func newClientStream(
	grpcStream core.LeverRPC_HandleStreamingRPCClient) (stream *ClientStream) {
	return &ClientStream{grpcStream: grpcStream}
}

// Send implements the Stream interface.
func (stream *ClientStream) Send(msg interface{}) error {
	byteMsg, isByteMsg := msg.([]byte)
	if isByteMsg {
		return stream.grpcStream.Send(&core.StreamMessage{
			MessageOneof: &core.StreamMessage_ByteMessage{
				ByteMessage: byteMsg,
			},
		})
	}

	encoded, err := encodeArg(msg)
	if err != nil {
		return err
	}
	return stream.grpcStream.Send(&core.StreamMessage{
		MessageOneof: &core.StreamMessage_Message{
			Message: encoded,
		},
	})
}

// Receive implements the Stream interface.
func (stream *ClientStream) Receive(msgObj interface{}) error {
	msg, err := stream.grpcStream.Recv()
	if err != nil {
		return err
	}

	if msg.GetMessageOneof() == nil {
		return fmt.Errorf("No message set in receive")
	}
	if msg.GetRpc() != nil {
		return fmt.Errorf("RPC field should be set only for first message")
	}
	switch msg := msg.GetMessageOneof().(type) {
	case *core.StreamMessage_ByteError:
		return &RemoteByteError{
			Err: msg.ByteError,
		}
	case *core.StreamMessage_Error:
		return &RemoteError{
			Err: buildDataForJSON(msg.Error),
		}
	case *core.StreamMessage_ByteMessage:
		byteMsgObj, ok := msgObj.(*[]byte)
		if !ok {
			return fmt.Errorf(
				"msgObj needs to be type *[]byte when receiving bytes")
		}
		*byteMsgObj = msg.ByteMessage
		return nil
	case *core.StreamMessage_Message:
		return decodeArgAsValue(msg.Message, msgObj)
	default:
		return fmt.Errorf("Invalid type")
	}
}

// Close implements the Stream interface.
func (stream *ClientStream) Close() error {
	return stream.grpcStream.CloseSend()
}

// Context implements the Stream interface.
func (stream *ClientStream) Context() context.Context {
	return stream.grpcStream.Context()
}

// ServerStream is the server Stream object that is provided as the first
// parameter in a Lever channel method.
type ServerStream struct {
	grpcStream core.LeverRPC_HandleStreamingRPCServer
}

func newServerStream(
	grpcStream core.LeverRPC_HandleStreamingRPCServer) (stream *ServerStream) {
	return &ServerStream{grpcStream: grpcStream}
}

// Send implements the Stream interface.
func (stream *ServerStream) Send(msg interface{}) error {
	byteMsg, isByteMsg := msg.([]byte)
	if isByteMsg {
		return stream.grpcStream.Send(&core.StreamMessage{
			MessageOneof: &core.StreamMessage_ByteMessage{
				ByteMessage: byteMsg,
			},
		})
	}

	encoded, err := encodeArg(msg)
	if err != nil {
		return err
	}
	return stream.grpcStream.Send(&core.StreamMessage{
		MessageOneof: &core.StreamMessage_Message{
			Message: encoded,
		},
	})
}

// Receive implements the Stream interface.
func (stream *ServerStream) Receive(msgObj interface{}) error {
	msg, err := stream.grpcStream.Recv()
	if err != nil {
		return err
	}

	if msg.GetMessageOneof() == nil {
		return fmt.Errorf("No message set in receive")
	}
	if msg.GetRpc() != nil {
		return fmt.Errorf("RPC field should be set only for first message")
	}
	switch msg := msg.GetMessageOneof().(type) {
	case *core.StreamMessage_ByteError:
		return &RemoteByteError{
			Err: msg.ByteError,
		}
	case *core.StreamMessage_Error:
		return &RemoteError{
			Err: buildDataForJSON(msg.Error),
		}
	case *core.StreamMessage_ByteMessage:
		byteMsgObj, ok := msgObj.(*[]byte)
		if !ok {
			return fmt.Errorf(
				"msgObj needs to be type *[]byte when receiving bytes")
		}
		*byteMsgObj = msg.ByteMessage
		return nil
	case *core.StreamMessage_Message:
		return decodeArgAsValue(msg.Message, msgObj)
	default:
		return fmt.Errorf("Invalid type")
	}
}

// Close implements the Stream interface.
func (stream *ServerStream) Close() error {
	// Not necessary on server side. Actual closing happens on handler return.
	return nil
}

// Context implements the Stream interface.
func (stream *ServerStream) Context() context.Context {
	return stream.grpcStream.Context()
}
