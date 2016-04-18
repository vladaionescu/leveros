package fleettracker

import (
	"github.com/leveros/leveros/scale"
	"golang.org/x/net/context"
)

// OnRPC calls the OnRPC method in the right shard of the fleettracker service.
func OnRPC(grpcPool *scale.GRPCPool, rpcEvent *RPCEvent) error {
	conn, sessionID, err := scale.InternalRPCConnResource(
		grpcPool, FleetTrackerService, rpcEvent.ServingID, TTLFlag.Get())
	if err != nil {
		return err
	}
	rpcEvent.SessionID = sessionID
	client := NewFleetTrackerClient(conn)
	_, err = client.HandleOnRPC(context.Background(), rpcEvent)
	return err
}
