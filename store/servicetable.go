package store

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	aerospike "github.com/aerospike/aerospike-client-go"
	"github.com/leveros/leveros/leverutil"
)

const serviceSet = "service"

// InitServiceTable initializes the indices for the service table.
func InitServiceTable(as *aerospike.Client) error {
	bin := "Env"
	task, err := as.CreateIndex(
		nil, leverOSNamespace, serviceSet, serviceSet+bin, bin,
		aerospike.STRING)
	if err != nil {
		return err
	}
	err, isErr := <-task.OnComplete()
	if isErr && err != nil {
		return err
	}
	return nil
}

// ServiceData is the data associated with the service and stored in the
// database.
type ServiceData struct {
	Env             string `as:"Env"`
	UniqueID        string `as:"ID"`
	Description     string `as:"Desc"`
	NextCodeVersion int64  `as:"NextVer"`
	LiveCodeVersion int64  `as:"LiveVer"`
	IsPublic        bool   `as:"IsPub"`
}

// NewService stores a new service.
func NewService(
	as *aerospike.Client, env string, service string,
	description string, isPublic bool) error {
	key, err := serviceKey(env, service)
	if err != nil {
		return err
	}
	wp := aerospike.NewWritePolicy(0, 0)
	wp.RecordExistsAction = aerospike.CREATE_ONLY
	return as.PutObject(wp, key, &ServiceData{
		Env: env,
		// TODO: This is not guaranteed to be unique. Should figure out a way
		//       to generate this uniquely.
		UniqueID:        leverutil.RandomHostName(),
		Description:     description,
		NextCodeVersion: 0,
		LiveCodeVersion: 0,
		IsPublic:        isPublic,
	})
}

// UpdateService updates the data associated with a service.
func UpdateService(
	as *aerospike.Client, env string, service string, description string,
	isPublic bool, liveCodeVersion int64) (
	err error) {
	key, err := serviceKey(env, service)
	if err != nil {
		return err
	}
	wp := aerospike.NewWritePolicy(0, 0)
	wp.RecordExistsAction = aerospike.UPDATE_ONLY
	return as.PutBins(
		wp, key,
		aerospike.NewBin("Desc", description),
		aerospike.NewBin("IsPub", btoi(isPublic)),
		aerospike.NewBin("LiveVer", liveCodeVersion),
	)
}

// NewServiceCodeVersion generates a version number of the code and returns it.
func NewServiceCodeVersion(
	as *aerospike.Client, env string, service string) (
	newVersion int64, err error) {
	key, err := serviceKey(env, service)
	if err != nil {
		return 0, err
	}

	bin := aerospike.NewBin("NextVer", 1)
	record, err := as.Operate(
		nil, key, aerospike.AddOp(bin),
		aerospike.GetOpForBin("NextVer"))
	if err != nil {
		return 0, err
	}

	return int64(record.Bins["NextVer"].(int)), nil
}

// SetServiceLiveCodeVersion sets the provided version of the code as the live
// one.
func SetServiceLiveCodeVersion(
	as *aerospike.Client, env string, service string, version int64) (
	err error) {
	key, err := serviceKey(env, service)
	if err != nil {
		return err
	}
	bin := aerospike.NewBin("LiveVer", version)
	return as.PutBins(nil, key, bin)
}

// ServiceExists returns true iff the service exists.
func ServiceExists(
	as *aerospike.Client, env string, service string) (
	exists bool, err error) {
	key, err := serviceKey(env, service)
	if err != nil {
		return false, err
	}
	return as.Exists(nil, key)
}

// ServiceServingData retrieves information about how to serve the code for this
// service.
//
// Note: This function is used on the hot path of RPC serving.
func ServiceServingData(
	as *aerospike.Client, env string, service string) (
	servingID string, liveVersion int64, isPublic bool, err error) {
	key, err := serviceKey(env, service)
	if err != nil {
		return "", 0, false, err
	}

	record, err := as.Get(nil, key, "ID", "LiveVer", "IsPub")
	if err != nil {
		return "", 0, false, err
	}
	if record == nil {
		return "", 0, false, fmt.Errorf("Service not found")
	}
	uniqueID := record.Bins["ID"].(string)
	liveVersion = int64(record.Bins["LiveVer"].(int))
	if liveVersion == 0 {
		return "", 0, false, fmt.Errorf("Service is not live")
	}
	isPublic = (record.Bins["IsPub"].(int) != 0)
	servingID = "lv--" + uniqueID + strconv.Itoa(int(liveVersion))
	return servingID, liveVersion, isPublic, nil
}

func serviceKey(env string, service string) (key *aerospike.Key, err error) {
	err = envNameOk(env)
	if err != nil {
		return nil, err
	}
	err = serviceNameOk(service)
	if err != nil {
		return nil, err
	}

	keyName := env + "/" + service
	return aerospike.NewKey(leverOSNamespace, serviceSet, keyName)
}

func serviceNameOk(service string) error {
	if len(service) < 3 {
		return fmt.Errorf("Service name too short")
	}
	if len(service) > 255 {
		return fmt.Errorf("Service name too long")
	}
	if service != url.QueryEscape(service) || strings.Contains(service, "/") {
		return fmt.Errorf("Service name not allowed")
	}
	return nil
}

func btoi(a bool) int {
	if a {
		return 1
	}
	return 0
}
