package admin

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	aerospike "github.com/aerospike/aerospike-client-go"
	leverapi "github.com/leveros/leveros/api"
	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/leverutil"
	"github.com/leveros/leveros/store"
)

// PackageName is the name of this package.
const PackageName = "admin"

var logger = leverutil.GetLogger(PackageName, "Admin")

const (
	codeBaseDir = "/leveros/custcodetree"

	maxUploadSize = 30 * 1024 * 1024 // 10 MiB
)

var (
	errInternal  = fmt.Errorf("Internal error")
	errTooLarge  = fmt.Errorf("File too large")
	errUntar     = fmt.Errorf("Error trying to untar file")
	errLeverJSON = fmt.Errorf("Error trying to read lever.json")
)

// Admin represents a Lever service that manages Lever environments and Lever
// services in a Lever deployment.
type Admin struct {
	as *aerospike.Client
}

// NewAdmin returns a new instance of Admin.
func NewAdmin(as *aerospike.Client) (*Admin, error) {
	return &Admin{as: as}, nil
}

// DeployServiceChan deploys the service with provided name onto Lever. If the
// service already exists, it is replaced. The method expects a code package
// in the form of chunks over stream messages to be sent over.
func (admin *Admin) DeployServiceChan(
	stream leverapi.Stream, envName string) error {
	// TODO: Auth layer.

	// Untar to a temp dir.
	tmpDir := tmpDir(envName)
	err := os.MkdirAll(tmpDir, 0777)
	if err != nil {
		logger.WithFields("err", err).Error(
			"Cannot create tmp dir for untar")
		return errInternal
	}

	success := false
	defer func() {
		if !success {
			err := os.RemoveAll(tmpDir)
			if err != nil {
				logger.WithFields("err", err).Error(
					"Error trying to remove tmp dir after failure to untar")
			}
		}
	}()

	pipeReader, pipeWriter := io.Pipe()
	bufReader := bufio.NewReader(pipeReader)
	doneCh := make(chan error, 1)
	go func() {
		err := leverutil.Untar(bufReader, tmpDir)
		if err != nil {
			doneCh <- err
		}
		close(doneCh)
	}()

	totalSize := 0
	for {
		var chunk []byte
		err := stream.Receive(&chunk)
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.WithFields("err", err).Error("Error receiving chunks")
			return errInternal
		}

		chunkSize, err := pipeWriter.Write(chunk)
		if err != nil {
			logger.WithFields("err", err).Error(
				"Error forwarding chunk to untar")
			return errInternal
		}

		// TODO: Also limit total size of decompressed files.
		totalSize += chunkSize
		if totalSize > maxUploadSize {
			logger.Error("File being uploaded is too large")
			return errTooLarge
		}

		// Check if there's been an untar error.
		select {
		case untarErr, hasErr := <-doneCh:
			if !hasErr {
				continue
			}
			logger.WithFields("err", untarErr).Error("Error trying to untar")
			return errUntar
		default:
		}
	}
	err = pipeWriter.Close()
	if err != nil {
		logger.WithFields("err", err).Error("Error closing pipe")
		return errInternal
	}
	// Wait for untar to finish.
	untarErr, hasErr := <-doneCh
	if hasErr && untarErr != nil {
		logger.WithFields("err", untarErr).Error("Error trying to untar")
		return errUntar
	}

	// Read lever.json.
	leverConfig, err := core.ReadLeverConfig(tmpDir)
	if err != nil {
		logger.WithFields("err", err).Error("Error trying to read lever.json")
		return errLeverJSON
	}

	// Init service table entry.
	createErr := store.NewService(
		admin.as, envName, leverConfig.Service, leverConfig.Description,
		!leverConfig.IsPrivate)
	// Ignore createErr here in case it already exists.
	codeVersion, err := store.NewServiceCodeVersion(
		admin.as, envName, leverConfig.Service)
	if err != nil {
		if createErr != nil {
			logger.WithFields("err", createErr).Error("Error creating service")
			return errInternal
		}
		logger.WithFields("err", err).Error("Error getting new service version")
		return errInternal
	}

	// Everything worked so far. Move the code to the right place.
	targetDir := codeDir(envName, leverConfig.Service, codeVersion)
	parentDir := filepath.Dir(targetDir)
	err = os.MkdirAll(parentDir, 0777)
	if err != nil {
		logger.WithFields("err", err).Error(
			"Cannot create target dir before move")
		return errInternal
	}
	err = os.Rename(tmpDir, targetDir)
	if err != nil {
		logger.WithFields("err", err).Error(
			"Error trying to move new service to its final destination")
		return errInternal
	}

	// Update entry in service table, making it point to the newly uploaded
	// version.
	err = store.UpdateService(
		admin.as, envName, leverConfig.Service, leverConfig.Description,
		!leverConfig.IsPrivate, codeVersion)
	if err != nil {
		logger.WithFields("err", err).Error("Error trying to update service")
		return errInternal
	}

	// TODO: Remove older versions of code to avoid having them pile up forever.
	success = true
	stream.Close()
	return nil
}

func codeDir(env string, service string, version int64) string {
	return filepath.Join(codeBaseDir, env, service, strconv.Itoa(int(version)))
}

func tmpDir(env string) string {
	return filepath.Join(
		codeBaseDir, env, ".tmp-"+leverutil.RandomID())
}
