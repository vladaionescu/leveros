package core

import (
	"fmt"
	"net/url"
	"strings"
)

// LeverURL holds information about a Lever RPC destination.
type LeverURL struct {
	Environment string
	Service     string
	Resource    string
	Method      string
}

// ParseLeverURL parses a URL into a LeverURL. Formats accepted are
//
// Absolute:
//     lever://<host>[:<port>]/<service>[/<resource>]/<method>
//
// or
//
// Relative (within same environment):
//     /<service>[/<resource>]/method
func ParseLeverURL(urlStr string) (*LeverURL, error) {
	parsed, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "" &&
		parsed.Scheme != "lever" {
		return nil, fmt.Errorf("Invalid scheme %v", parsed.Scheme)
	}
	path := parsed.EscapedPath()
	if path != "" && path[0] == '/' {
		path = path[1:]
	}
	firstSep := strings.Index(path, "/")
	lastSep := strings.LastIndex(path, "/")
	leverURL := &LeverURL{
		Environment: parsed.Host,
	}
	if firstSep == -1 {
		return nil, fmt.Errorf("Invalid Lever URL")
	}
	leverURL.Service = path[:firstSep]
	if firstSep != lastSep {
		leverURL.Resource = path[firstSep+1 : lastSep]
	}
	leverURL.Method = path[lastSep+1:]
	return leverURL, nil
}

// String returns the URL string representation of the LeverURL.
// If Environment is "" then the URL will be relative.
func (leverURL *LeverURL) String() string {
	if leverURL.Environment == "" {
		return fmt.Sprintf(
			"/%s/%s/%s", leverURL.Service, leverURL.Resource, leverURL.Method)
	}
	return fmt.Sprintf(
		"lever://%s/%s/%s/%s", leverURL.Environment, leverURL.Service,
		leverURL.Resource, leverURL.Method)
}
