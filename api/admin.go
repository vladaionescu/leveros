package api

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/leveros/leveros/core"
	"github.com/leveros/leveros/leverutil"
)

// DeployServiceDir deploys provided dir onto Lever, as a Lever service.
func DeployServiceDir(
	adminEnv string, host string, env string, srcDir string) error {
	client, err := NewClient()
	if err != nil {
		return err
	}
	if host != "" {
		client.ForceHost = host
	}

	info, err := os.Lstat(srcDir)
	if err != nil {
		return fmt.Errorf("Error getting info about source dir: %v", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("Source is not a directory")
	}

	_, err = core.ReadLeverConfig(srcDir)
	if err != nil {
		return fmt.Errorf("Error reading lever.json: %v", err)
	}

	pipeReader, pipeWriter := io.Pipe()
	bufReader := bufio.NewReader(pipeReader)
	errCh := make(chan error)
	go func() {
		tarErr := leverutil.Tar(pipeWriter, srcDir)
		if tarErr != nil {
			errCh <- fmt.Errorf("Error trying to tar dir: %v", tarErr)
			return
		}
		tarErr = pipeWriter.Close()
		if tarErr != nil {
			errCh <- tarErr
		}
		close(errCh)
	}()

	endpoint := client.Service(adminEnv, "admin")
	stream, err := endpoint.InvokeChan("DeployServiceChan", env)
	if err != nil {
		return fmt.Errorf("Error invoking DeployServiceChan: %v", err)
	}

	chunk := make([]byte, 32*1024)
	for {
		var size int
		size, err = bufReader.Read(chunk)
		if err == io.EOF {
			break
		}
		if err != nil {
			stream.Close()
			return err
		}

		err = stream.Send(chunk[:size])
		if err != nil {
			stream.Close()
			return err
		}
	}
	err = stream.Close()
	if err != nil {
		return err
	}
	err, hasErr := <-errCh
	if hasErr && err != nil {
		return err
	}
	// Wait for remote to close stream.
	var msg interface{}
	err = stream.Receive(&msg)
	if err == nil {
		return fmt.Errorf("Unexpected message received")
	}
	if err != nil && err != io.EOF {
		return err
	}
	return nil
}
