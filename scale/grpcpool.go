package scale

import (
	"time"

	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/leverutil"
	"google.golang.org/grpc"
)

var (
	// GRPCUnusedTimeoutFlag is the time a connection is dropped after it has
	// been last used.
	GRPCUnusedTimeoutFlag = config.DeclareDuration(
		PackageName, "grpcUnusedTimeout", 5*time.Minute)
)

// GRPCPool represents a pool of GRPC client connections.
type GRPCPool struct {
	conns *leverutil.Cache
}

// NewGRPCPool returns a new GRPC connection pool.
func NewGRPCPool() (*GRPCPool, error) {
	expiry := GRPCUnusedTimeoutFlag.Get()
	pool := new(GRPCPool)
	pool.conns = leverutil.NewCache(
		expiry,
		func(addr string) (interface{}, error) {
			return grpc.Dial(addr, grpc.WithInsecure())
		},
		func(conn interface{}) {
			if conn == nil {
				return
			}
			conn.(*grpc.ClientConn).Close()
		},
	)
	return pool, nil
}

// Dial returns a (potentially cached) GRPC connection associated with target.
func (pool *GRPCPool) Dial(target string) (*grpc.ClientConn, error) {
	conn, err := pool.conns.Get(target)
	if err != nil {
		return nil, err
	}
	return conn.(*grpc.ClientConn), nil
}

// KeepAlive resets the target's timer to prevent the connection
// from expiring and being closed down by the pool.
func (pool *GRPCPool) KeepAlive(target string) {
	pool.conns.KeepAlive(target)
}
