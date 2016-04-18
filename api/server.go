package api

import (
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	"github.com/leveros/leveros/core"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type handlerEntry struct {
	handler           interface{}
	isMethod          bool
	obj               interface{}
	hasContext        bool
	isByteResult      bool
	isByteErrorResult bool
}

// Server is a server that handles Lever RPCs.
type Server struct {
	serviceName string
	listener    net.Listener
	grpcServer  *grpc.Server

	// Guard the following.
	lock sync.RWMutex
	// Handlers for RPCs.
	handlers map[string]*handlerEntry
	// Handlers for streaming RPCs.
	streamingHandlers map[string]*handlerEntry
	// The resources currently being served by this instance.
	resources map[string]struct{}
}

// NewServer creates a new Server.
func NewServer() (server *Server, err error) {
	server = &Server{
		serviceName:       OwnService,
		handlers:          make(map[string]*handlerEntry),
		streamingHandlers: make(map[string]*handlerEntry),
	}
	listenAddr := ":" + core.InstanceListenPortFlag.Get()
	server.listener, err = net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	server.grpcServer = grpc.NewServer()
	server.grpcServer.RegisterService(
		core.NewServiceDesc(OwnService, ""), server)
	return server, nil
}

// Serve enters the Server's serving loop. This blocks until the server no
// longer serves.
func (server *Server) Serve() error {
	return server.grpcServer.Serve(server.listener)
}

// Stop stops a Server that is currently serving.
func (server *Server) Stop() {
	server.grpcServer.Stop()
}

// RegisterHandlerObject registers an object to handle RPCs with method names
// that are the same as the methods of the provided object. Private methods
// are ignored.
func (server *Server) RegisterHandlerObject(obj interface{}) error {
	objType := reflect.TypeOf(obj)
	for index := 0; index < objType.NumMethod(); index++ {
		method := objType.Method(index)
		name := method.Name
		if len(name) == 0 || strings.ToUpper(name[:1]) != name[:1] {
			// Ignore non-exported methods.
			continue
		}
		err := server.registerHandlerInternal(
			name, method.Func.Interface(), true, obj)
		if err != nil {
			return err
		}
	}
	return nil
}

// RegisterHandler registers a function to handle the method with provided name.
func (server *Server) RegisterHandler(
	name string, handler interface{}) error {
	return server.registerHandlerInternal(name, handler, false, nil)
}

func (server *Server) registerHandlerInternal(
	name string, handler interface{}, isMethod bool, obj interface{}) error {
	funcType := reflect.TypeOf(handler)
	if funcType.Kind() != reflect.Func {
		return fmt.Errorf("Handler needs to be a function")
	}
	if funcType.NumIn() < 1 {
		return fmt.Errorf("Invalid params for method %s", name)
	}
	contextIndex := 0
	if isMethod {
		contextIndex = 1
	}
	hasContext := false
	if funcType.NumIn() > contextIndex &&
		isType(
			funcType.In(contextIndex), "golang.org/x/net/context", "Context") {
		hasContext = true
	}

	isStreaming := IsChanMethod(name)
	if (!isStreaming && funcType.NumOut() != 2) ||
		(isStreaming && funcType.NumOut() != 1) {
		return fmt.Errorf("Invalid number of return params")
	}

	isByteResult := false
	if !isStreaming {
		out0 := funcType.Out(0)
		if out0.Kind() == reflect.Slice && out0.Elem().Kind() == reflect.Uint8 {
			isByteResult = true
		}
	}

	errorIndex := 1
	if isStreaming {
		errorIndex = 0
	}

	isByteErrorResult := false
	outErr := funcType.Out(errorIndex)
	if outErr.Kind() == reflect.Slice && outErr.Elem().Kind() == reflect.Uint8 {
		isByteErrorResult = true
	} else if outErr.Kind() == reflect.Interface &&
		isType(outErr, "github.com/leveros/leveros/api", "BytesError") {
		isByteErrorResult = true
	}

	entry := &handlerEntry{
		handler:           handler,
		isMethod:          isMethod,
		obj:               obj,
		hasContext:        hasContext,
		isByteResult:      isByteResult,
		isByteErrorResult: isByteErrorResult,
	}
	server.lock.Lock()
	defer server.lock.Unlock()
	if isStreaming {
		_, exists := server.streamingHandlers[name]
		if exists {
			return fmt.Errorf("Handler already exists")
		}
		server.streamingHandlers[name] = entry
	} else {
		_, exists := server.handlers[name]
		if exists {
			return fmt.Errorf("Handler already exists")
		}
		server.handlers[name] = entry
	}
	return nil
}

// HandleRPC is an internal method. Do not use!
func (server *Server) HandleRPC(
	ctx context.Context, rpc *core.RPC) (reply *core.RPCReply, err error) {
	err = setInternalRPCGateway(ctx)
	if err != nil {
		return nil, err
	}

	if rpc.GetArgsOneof() == nil {
		return nil, fmt.Errorf("RPC has no args oneof")
	}

	resource := extractResource(ctx)
	server.lock.RLock()
	entry, handlerOK := server.handlers[rpc.Method]
	resourceOK := true
	if resource != "" {
		_, resourceOK = server.resources[resource]
	}
	server.lock.RUnlock()
	if !handlerOK {
		return nil, fmt.Errorf("Method not found")
	}
	if !resourceOK {
		return nil, fmt.Errorf("Resource not found")
	}

	if resource == "" {
		err = server.maybeHandleResourceLifecycle(rpc)
		if err != nil {
			return nil, err
		}
	}

	return callHandler(ctx, resource, entry, rpc, nil)
}

// HandleStreamingRPC is an internal method. Do not use!
func (server *Server) HandleStreamingRPC(
	grpcStream core.LeverRPC_HandleStreamingRPCServer) error {
	ctx := grpcStream.Context()
	err := setInternalRPCGateway(ctx)
	if err != nil {
		return err
	}

	fstMsg, err := grpcStream.Recv()
	if err != nil {
		return err
	}
	rpc := fstMsg.GetRpc()
	if rpc == nil {
		return fmt.Errorf("First message needs to have RPC field set")
	}

	if rpc.GetArgsOneof() == nil {
		return fmt.Errorf("RPC has no args oneof")
	}

	resource := extractResource(ctx)
	server.lock.RLock()
	entry, handlerOK := server.streamingHandlers[rpc.Method]
	resourceOK := true
	if resource != "" {
		_, resourceOK = server.resources[resource]
	}
	server.lock.RUnlock()
	if !handlerOK {
		return fmt.Errorf("Method not found")
	}
	if !resourceOK {
		return fmt.Errorf("Resource not found")
	}

	_, err = callHandler(ctx, resource, entry, rpc, grpcStream)
	return err
}

func callHandler(
	ctx context.Context, resource string, entry *handlerEntry, rpc *core.RPC,
	grpcStream core.LeverRPC_HandleStreamingRPCServer) (
	reply *core.RPCReply, err error) {
	// Prepare args for handler.
	var callArgs []reflect.Value
	if entry.isMethod {
		callArgs = append(callArgs, reflect.ValueOf(entry.obj))
	}
	isStreaming := (grpcStream != nil)
	if isStreaming {
		callArgs = append(
			callArgs, reflect.ValueOf(newServerStream(grpcStream)))
	} else {
		if entry.hasContext {
			callArgs = append(callArgs, reflect.ValueOf(ctx))
		}
	}
	if resource != "" {
		callArgs = append(callArgs, reflect.ValueOf(resource))
	}
	switch args := rpc.GetArgsOneof().(type) {
	case *core.RPC_Args:
		var values []reflect.Value
		values, err = decodeArgsAsValue(args.Args, entry, isStreaming)
		if err != nil {
			return nil, err
		}
		callArgs = append(callArgs, values...)
	case *core.RPC_ByteArgs:
		callArgs = append(callArgs, reflect.ValueOf(args.ByteArgs))
	default:
		return nil, fmt.Errorf("Invalid args")
	}

	// Call handler.
	result := reflect.ValueOf(entry.handler).Call(callArgs)

	// Interpret result.
	errIndex := 1
	if isStreaming {
		errIndex = 0
	}
	if entry.isByteErrorResult {
		var errBytesResult []byte
		v := result[errIndex].Interface()
		if v != nil {
			var ok bool
			errBytesResult, ok = result[errIndex].Interface().([]byte)
			if !ok {
				errBytesResult =
					result[errIndex].Interface().(BytesError).GetBytes()
			}
			if errBytesResult != nil {
				if !isStreaming {
					return &core.RPCReply{
						ResultOneof: &core.RPCReply_ByteError{
							ByteError: errBytesResult,
						},
					}, nil
				}
				err = grpcStream.Send(&core.StreamMessage{
					MessageOneof: &core.StreamMessage_ByteError{
						ByteError: errBytesResult,
					},
				})
				return nil, err
			}
		}
	} else {
		v := result[errIndex].Interface()
		if v != nil {
			theError, isErrorType := v.(error)
			if isErrorType {
				// For error type, just use the string returned by Error().
				v = theError.Error()
			}
			var encodedErr *core.JSON
			encodedErr, err = encodeArg(v)
			if err != nil {
				return nil, err
			}
			if !isStreaming {
				return &core.RPCReply{
					ResultOneof: &core.RPCReply_Error{
						Error: encodedErr,
					},
				}, nil
			}
			err = grpcStream.Send(&core.StreamMessage{
				MessageOneof: &core.StreamMessage_Error{
					Error: encodedErr,
				},
			})
			return nil, err
		}
	}

	if isStreaming {
		// Stream case - no reply sent back.
		return nil, nil
	}

	// Non-stream case - build reply.
	if entry.isByteResult {
		return &core.RPCReply{
			ResultOneof: &core.RPCReply_ByteResult{
				ByteResult: result[0].Interface().([]byte),
			},
		}, nil
	}
	encodedResult, err := encodeArg(result[0].Interface())
	if err != nil {
		return nil, err
	}
	return &core.RPCReply{
		ResultOneof: &core.RPCReply_Result{
			Result: encodedResult,
		},
	}, nil
}

func (server *Server) maybeHandleResourceLifecycle(rpc *core.RPC) error {
	switch rpc.Method {
	case "NewResource":
		resource, err := extractResourceFromArgs(rpc)
		if err != nil {
			return err
		}

		server.lock.Lock()
		defer server.lock.Unlock()
		_, alreadyExists := server.resources[resource]
		if alreadyExists {
			return fmt.Errorf("Resource already exists")
		}

		server.resources[resource] = struct{}{}
		// Bind to the same instance. We will differentiate between resources
		// at serving time by extracting the resource name from headers.
		server.grpcServer.RegisterService(
			core.NewServiceDesc(OwnService, resource), server)
	case "CloseResource":
		resource, err := extractResourceFromArgs(rpc)
		if err != nil {
			return err
		}

		server.lock.Lock()
		defer server.lock.Unlock()
		_, exists := server.resources[resource]
		if !exists {
			return fmt.Errorf("Resource does not exist")
		}

		delete(server.resources, resource)
		grpcServiceName := OwnService + "/" + resource
		server.grpcServer.DeregisterService(grpcServiceName)
	default:
	}
	return nil
}

func extractResource(ctx context.Context) (resource string) {
	md, ok := metadata.FromContext(ctx)
	if !ok {
		return ""
	}
	resourceHeader, ok := md["x-lever-resource"]
	if !ok || len(resourceHeader) == 0 {
		return ""
	}
	return resourceHeader[0]
}

func extractResourceFromArgs(rpc *core.RPC) (resource string, err error) {
	if rpc.GetArgsOneof() == nil {
		return "", fmt.Errorf("Could not find args")
	}
	args, ok := rpc.GetArgsOneof().(*core.RPC_Args)
	if !ok {
		return "", fmt.Errorf("Unexpected type of args for newResource")
	}
	if len(args.Args.Element) != 1 {
		return "", fmt.Errorf("Unexpected number of args for newResource")
	}
	el := args.Args.Element[0]
	if el.GetJsonValueOneof() == nil {
		return "", fmt.Errorf("Unexpected arg for newResource")
	}
	strArg, ok := el.GetJsonValueOneof().(*core.JSON_JsonString)
	if !ok {
		return "", fmt.Errorf("Unexpected arg type for newResource")
	}
	resource = strArg.JsonString
	if resource == "" {
		return "", fmt.Errorf("Invalid resource empty string")
	}
	return resource, nil
}

func isType(t reflect.Type, packageName string, typeName string) bool {
	if typeName != t.Name() {
		return false
	}
	pkgPath := strings.Split(t.PkgPath(), string(filepath.Separator))
	lastVendor := -1
	for i := len(pkgPath) - 1; i >= 0; i-- {
		if pkgPath[i] == "vendor" {
			lastVendor = i
			break
		}
	}
	if lastVendor != -1 {
		pkgPath = pkgPath[lastVendor+1:]
	}
	return filepath.Join(pkgPath...) == packageName
}
