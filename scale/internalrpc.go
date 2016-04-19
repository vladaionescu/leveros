package scale

import (
	"time"

	grpc "github.com/leveros/grpc-go"
)

// InternalRPCConn creates a GRPC connection to an internal service.
func InternalRPCConn(
	grpcPool *GRPCPool, service string) (conn *grpc.ClientConn, err error) {
	target, _, err := DereferenceService(service)
	if err != nil {
		return nil, err
	}
	return grpcPool.Dial(target)
}

// InternalRPCConnResource creates a GRPC connection to an internal resource.
// If the resource does dot exist, it is assigned atomically to an instance in
// the service.
func InternalRPCConnResource(
	grpcPool *GRPCPool, service string, resource string, ttl time.Duration) (
	conn *grpc.ClientConn, sessionID string, err error) {
	res, _, err := DereferenceOrRegisterResource(service, resource, ttl)
	if err != nil {
		return nil, "", err
	}
	conn, err = grpcPool.Dial(res.target)
	if err != nil {
		return nil, "", err
	}
	return conn, res.GetSessionID(), nil
}
