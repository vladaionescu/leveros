package core

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// LeverConfig is the representation of the contents of lever.json.
type LeverConfig struct {
	Service     string `json:"name"`
	Description string `json:"description,omitempty"`

	// Only one may be specified.
	EntryPoint   []string `json:"entry,omitempty"`
	JSEntryPoint string   `json:"jsEntry,omitempty"`

	IsPrivate bool `json:"private,omitempty"`

	MaxInstanceLoad float64 `json:"maxInstanceLoad,omitempty"`
	MinInstances    int     `json:"minInstances,omitempty"`

	// TODO: Future: Allow cust to add own config which can be read from within
	//       the service. Changing just these would not cause service restart.
	//       Think feature flags.
	CustConfig map[string]interface{} `json:"config,omitempty"`
}

// ReadLeverConfig reads lever.json from the provided directory.
func ReadLeverConfig(dirPath string) (*LeverConfig, error) {
	leverJSONPath := filepath.Join(dirPath, "lever.json")
	file, err := os.Open(leverJSONPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	leverConfig := new(LeverConfig)
	err = dec.Decode(leverConfig)
	if err != nil {
		return nil, err
	}
	if leverConfig.MaxInstanceLoad == 0.0 {
		leverConfig.MaxInstanceLoad = 10.0
	}
	if leverConfig.CustConfig == nil {
		leverConfig.CustConfig = make(map[string]interface{})
	}

	if leverConfig.Service == "" {
		return nil, fmt.Errorf("Invalid service name")
	}

	entryPoints := 0
	if len(leverConfig.EntryPoint) > 0 {
		entryPoints++
	}
	if leverConfig.JSEntryPoint != "" {
		entryPoints++
	}
	if entryPoints > 1 {
		return nil, fmt.Errorf("More than one type of entry point specified")
	}
	if entryPoints == 0 {
		leverConfig.EntryPoint = []string{"./serve"}
	}
	return leverConfig, nil
}

// Write writes lever.json to provied directory.
func (leverConfig *LeverConfig) Write(dirPath string) error {
	leverJSONPath := filepath.Join(dirPath, "lever.json")
	file, err := os.Create(leverJSONPath)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(file)
	err = enc.Encode(leverConfig)
	if err != nil {
		file.Close()
		os.Remove(leverJSONPath)
		return err
	}
	file.Close()
	return nil
}

// GeneralEntryPoint returns the entry point specified by the config.
// (Whether that is specified via entry, goEntry etc).
func (leverConfig *LeverConfig) GeneralEntryPoint() []string {
	if len(leverConfig.EntryPoint) > 0 {
		return leverConfig.EntryPoint
	}
	if leverConfig.JSEntryPoint != "" {
		return []string{"node", leverConfig.JSEntryPoint}
	}
	panic(fmt.Errorf("Invalid config - no entry point"))
}
