package store

import (
	"fmt"
	"net/url"

	aerospike "github.com/aerospike/aerospike-client-go"
)

const envSet = "env"

// EnvData is the data associated with the env and stored in the database.
type EnvData struct {
	Description string `as:"Description"`
}

// NewEnv stores a new env.
func NewEnv(as *aerospike.Client, env string, description string) error {
	key, err := envKey(env)
	if err != nil {
		return err
	}
	wp := aerospike.NewWritePolicy(0, 0)
	wp.RecordExistsAction = aerospike.CREATE_ONLY
	return as.PutObject(wp, key, &EnvData{Description: description})
}

// DeleteEnv removes an env.
func DeleteEnv(as *aerospike.Client, env string) error {
	key, err := envKey(env)
	if err != nil {
		return err
	}
	_, err = as.Delete(nil, key)
	return err
}

// EnvExists returns true iff the env exists.
func EnvExists(as *aerospike.Client, env string) (exists bool, err error) {
	key, err := envKey(env)
	if err != nil {
		return false, err
	}
	return as.Exists(nil, key)
}

func envKey(env string) (key *aerospike.Key, err error) {
	err = envNameOk(env)
	if err != nil {
		return nil, err
	}
	return aerospike.NewKey(leverOSNamespace, envSet, env)
}

func envNameOk(env string) error {
	if len(env) < 3 {
		return fmt.Errorf("Env name too short")
	}
	if env != url.QueryEscape(env) {
		return fmt.Errorf("Env name not allowed")
	}
	return nil
}
