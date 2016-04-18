package hostman

import (
	"github.com/leveros/leveros/scale"
	"golang.org/x/net/context"
)

// InitializeInstance initializes the infrastructure necessary for instance to
// start serving.
func InitializeInstance(
	grpcPool *scale.GRPCPool, info *InstanceInfo, node string) error {
	res, err := scale.DereferenceResource(
		ManagerService, "swarm://"+node)
	if err != nil {
		return err
	}
	conn, err := grpcPool.Dial(res.GetTarget())
	if err != nil {
		return err
	}
	client := NewManagerClient(conn)
	_, err = client.InitializeInstance(context.Background(), info)
	return err
}

// StopInstance stops a given Lever instance.
func StopInstance(
	grpcPool *scale.GRPCPool, key *InstanceKey, node string) error {
	res, err := scale.DereferenceResource(
		ManagerService, "swarm://"+node)
	if err != nil {
		return err
	}
	conn, err := grpcPool.Dial(res.GetTarget())
	if err != nil {
		return err
	}
	client := NewManagerClient(conn)
	_, err = client.StopInstance(context.Background(), key)
	return err
}
