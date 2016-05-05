// +build integration

package systemtest

import (
	"io"
	"os"
	"testing"

	"github.com/leveros/leveros/api"
	"github.com/leveros/leveros/api/admin"
	"github.com/leveros/leveros/config"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/leverutil"
	"github.com/stretchr/testify/assert"
)

const serviceName = "basictestservice"

var (
	client       *api.Client
	leverService *api.Endpoint
)

func TestMain(m *testing.M) {
	config.Initialize()
	leverutil.UpdateLoggingSettings()

	logger.Info("Deploying...")
	err := admin.DeployServiceDir(
		core.AdminEnvFlag.Get(), core.DefaultDevAliasFlag.Get(),
		core.DefaultDevEnvFlag.Get(), serviceName)
	if err != nil {
		logger.WithFields("err", err).Fatal("Error when deploying")
	}
	logger.Info("Deployed")
	client, err = api.NewClient()
	if err != nil {
		logger.WithFields("err", err).Fatal("Error getting client")
	}
	client.ForceHost = core.DefaultDevAliasFlag.Get()
	leverService = client.Service(core.DefaultDevEnvFlag.Get(), serviceName)
	logger.Info("Running tests...")
	os.Exit(m.Run())
}

func TestBasicHello(t *testing.T) {
	var reply string
	err := leverService.Invoke(
		&reply, "BasicHello", "name")
	assert.NoError(t, err)
	assert.Equal(t, "hello name", reply)
}

func TestReqReplyStream(t *testing.T) {
	stream, err := leverService.InvokeChan("ReqReplyStreamChan", "some test")
	assert.NoError(t, err)
	messages := []string{"1", "2", "hello", "world", "test", "123"}
	for _, msg := range messages {
		err = stream.Send(msg)
		assert.NoError(t, err)
		var reply string
		err = stream.Receive(&reply)
		assert.NoError(t, err)
		assert.Equal(t, "some test:"+msg+" received", reply)
	}
	err = stream.Close()
	assert.NoError(t, err)
	var reply interface{}
	err = stream.Receive(&reply)
	assert.Equal(t, io.EOF, err)
}

func TestBasicError(t *testing.T) {
	var reply interface{}
	err := leverService.Invoke(&reply, "BasicError", "some test")
	assert.EqualError(t, err, "error sent: some test")
}
