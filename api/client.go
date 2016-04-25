package api

import (
	"fmt"
	"os"
	"sync"

	"github.com/leveros/grpc-go/metadata"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/scale"
	"golang.org/x/net/context"
)

var (
	internalRPCGatewayLock sync.RWMutex
	internalRPCGateway     = os.Getenv("LEVEROS_INTERNAL_RPC_GATEWAY")
)

func setInternalRPCGateway(ctx context.Context) error {
	// TODO: This function is only necessary as part of a hack to set the
	// RPC endpoint as part of the first incoming RPC. It shouldn't be necessary
	// to do that. The correct value should be set via env vars from the start.
	internalRPCGatewayLock.RLock()
	if internalRPCGateway != "" {
		internalRPCGatewayLock.RUnlock()
		return nil
	}
	internalRPCGatewayLock.RUnlock()

	md, ok := metadata.FromContext(ctx)
	if !ok {
		// No metadata. Don't set it.
		return nil
	}
	gateway, ok := md["x-lever-internal-rpc-gateway"]
	if !ok || len(gateway) == 0 {
		// Header not found. Don't set it.
		return nil
	}
	if gateway[0] != "" {
		internalRPCGatewayLock.Lock()
		internalRPCGateway = gateway[0]
		internalRPCGatewayLock.Unlock()
	}
	return nil
}

// RemoteByteError is an error received as bytes after calling a remote method.
type RemoteByteError struct {
	Err []byte
}

func (err *RemoteByteError) Error() string {
	return string(err.Err)
}

// GetBytes returns the error as bytes.
func (err *RemoteByteError) GetBytes() []byte {
	return err.Err
}

// RemoteError is an error received after calling a remote method.
type RemoteError struct {
	Err interface{}
}

func (err *RemoteError) Error() string {
	return fmt.Sprintf("%v", err.Err)
}

// Endpoint represents a Lever service or a Lever resource within a service.
// It can be used to invoke methods remotely.
type Endpoint struct {
	leverURL *core.LeverURL
	client   *Client
}

// Invoke invokes a remote method and populates replyObj with the response if
// successful.
func (endpoint *Endpoint) Invoke(
	replyObj interface{}, method string, args ...interface{}) (err error) {
	invokationURL := &core.LeverURL{
		Environment: endpoint.leverURL.Environment,
		Service:     endpoint.leverURL.Service,
		Resource:    endpoint.leverURL.Resource,
		Method:      method,
	}
	return endpoint.client.invokeInternal(
		replyObj, invokationURL, args...)
}

// InvokeChan invokes a remote streaming method. It returns a Stream object
// which can be used to communicate with the Lever instance in real-time.
func (endpoint *Endpoint) InvokeChan(
	method string, args ...interface{}) (stream Stream, err error) {
	invokationURL := &core.LeverURL{
		Environment: endpoint.leverURL.Environment,
		Service:     endpoint.leverURL.Service,
		Resource:    endpoint.leverURL.Resource,
		Method:      method,
	}
	return endpoint.client.invokeChanInternal(invokationURL, args...)
}

// Client is a Lever OS client. It can be used to initiate RPC's to other
// lever services.
type Client struct {
	conns     *scale.GRPCPool
	ForceHost string
}

// NewClient creates a new Lever Client.
func NewClient() (*Client, error) {
	conns, err := scale.NewGRPCPool()
	if err != nil {
		return nil, err
	}
	return &Client{conns: conns}, nil
}

// Service returns an endpoint representing a Lever service. If env is "" then
// the current environment is used (assuming you are running within a Lever
// instance).
func (client *Client) Service(env string, service string) *Endpoint {
	return &Endpoint{
		leverURL: &core.LeverURL{
			Environment: env,
			Service:     service,
		},
		client: client,
	}
}

// Resource returns an endpoint representing a Lever resource within a Lever
// service. If env is "" then the current environment is used (assuming you are
// running within a Lever instance).
func (client *Client) Resource(
	env string, service string, resource string) *Endpoint {
	return &Endpoint{
		leverURL: &core.LeverURL{
			Environment: env,
			Service:     service,
			Resource:    resource,
		},
		client: client,
	}
}

// InvokeURL invokes a remote method referenced by a Lever URL. The URL can
// be either absolute (remote environment) or relative (same environment).
// Example:
//
// /<service>[/<resource>]/method  (relative URL)
//
// lever://<env>/<service>[/<resource>]/method  (absolute URL)
func (client *Client) InvokeURL(
	replyObj interface{}, leverURLStr string, args ...interface{}) (err error) {
	leverURL, err := core.ParseLeverURL(leverURLStr)
	if err != nil {
		return err
	}
	return client.invokeInternal(replyObj, leverURL, args...)
}

// InvokeChanURL invokes a remote streaming method referenced by a Lever URL.
// The URL can be either absolute (remote environment) or relative
// (same environment).
// Example:
//
// /<service>[/<resource>]/method  (relative URL)
//
// lever://<env>/<service>[/<resource>]/method  (absolute URL)
func (client *Client) InvokeChanURL(
	leverURLStr string, args ...interface{}) (stream Stream, err error) {
	leverURL, err := core.ParseLeverURL(leverURLStr)
	if err != nil {
		return nil, err
	}
	return client.invokeChanInternal(leverURL, args...)
}

func (client *Client) invokeInternal(
	replyObj interface{}, leverURL *core.LeverURL,
	args ...interface{}) (err error) {
	if IsChanMethod(leverURL.Method) {
		return fmt.Errorf(
			"Use InvokeChan / InvokeChanURL for streaming methods")
	}
	if leverURL.Environment == "" && OwnEnvironment == "" {
		return fmt.Errorf(
			"Environment not specified and cannot be deduced")
	}
	if leverURL.Environment == "" {
		leverURL.Environment = OwnEnvironment
	}

	var dialTo string
	if client.ForceHost != "" {
		dialTo = client.ForceHost
	} else {
		if (core.IsInternalEnvironment(leverURL.Environment) ||
			leverURL.Environment == OwnEnvironment) &&
			internalRPCGateway != "" {
			dialTo = internalRPCGateway
		} else {
			dialTo = leverURL.Environment
		}
	}
	conn, err := client.conns.Dial(dialTo)
	if err != nil {
		return err
	}

	rpc := &core.RPC{}
	if len(args) == 1 {
		byteArgs, ok := args[0].([]byte)
		if ok {
			// Byte args case.
			rpc.ArgsOneof = &core.RPC_ByteArgs{
				ByteArgs: byteArgs,
			}
		}
	}
	if rpc.ArgsOneof == nil {
		// Non-byte args case.
		var encdArgs *core.JSONArray
		encdArgs, err = encodeArgs(args)
		if err != nil {
			return err
		}
		rpc.ArgsOneof = &core.RPC_Args{
			Args: encdArgs,
		}
	}
	reply, err := core.SendLeverRPC(
		conn, context.Background(), leverURL, rpc)
	if err != nil {
		return err
	}

	if reply.GetResultOneof() == nil {
		return fmt.Errorf("Received nil result oneof")
	}
	switch result := reply.GetResultOneof().(type) {
	case *core.RPCReply_ByteResult:
		byteReplyObj, ok := replyObj.(*[]byte)
		if !ok {
			return fmt.Errorf(
				"replyObj needs to be type *[]byte when RPC returns bytes")
		}
		*byteReplyObj = result.ByteResult
		return nil
	case *core.RPCReply_ByteError:
		return &RemoteByteError{Err: result.ByteError}
	case *core.RPCReply_Result:
		return decodeArgAsValue(result.Result, replyObj)
	case *core.RPCReply_Error:
		return &RemoteError{Err: buildDataForJSON(result.Error)}
	default:
		return fmt.Errorf("Invalid type")
	}
}

func (client *Client) invokeChanInternal(
	leverURL *core.LeverURL, args ...interface{}) (stream Stream, err error) {
	if !IsChanMethod(leverURL.Method) {
		return nil, fmt.Errorf(
			"Use Invoke / InvokeURL for non-streaming methods")
	}
	if leverURL.Environment == "" && OwnEnvironment == "" {
		return nil, fmt.Errorf(
			"Environment not specified and cannot be deduced")
	}
	if leverURL.Environment == "" {
		leverURL.Environment = OwnEnvironment
	}

	var dialTo string
	if client.ForceHost != "" {
		dialTo = client.ForceHost
	} else {
		if (core.IsInternalEnvironment(leverURL.Environment) ||
			leverURL.Environment == OwnEnvironment) &&
			internalRPCGateway != "" {
			dialTo = internalRPCGateway
		} else {
			dialTo = leverURL.Environment
		}
	}
	conn, err := client.conns.Dial(dialTo)
	if err != nil {
		return nil, err
	}

	rpc := &core.RPC{}
	if len(args) == 1 {
		byteArgs, ok := args[0].([]byte)
		if ok {
			// Byte args case.
			rpc.ArgsOneof = &core.RPC_ByteArgs{
				ByteArgs: byteArgs,
			}
		}
	}
	if rpc.ArgsOneof == nil {
		// Non-byte args case.
		var encdArgs *core.JSONArray
		encdArgs, err = encodeArgs(args)
		if err != nil {
			return nil, err
		}
		rpc.ArgsOneof = &core.RPC_Args{
			Args: encdArgs,
		}
	}
	grpcStream, err := core.SendStreamingLeverRPC(
		conn, context.Background(), leverURL)
	if err != nil {
		return nil, err
	}

	err = grpcStream.Send(&core.StreamMessage{
		MessageOneof: &core.StreamMessage_Rpc{
			Rpc: rpc,
		},
	})
	if err != nil {
		grpcStream.CloseSend()
		return nil, err
	}
	return newClientStream(grpcStream), nil
}
