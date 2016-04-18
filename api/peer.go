package api

import (
	"fmt"
	"net/url"
	"strings"

	"golang.org/x/net/context"
)

// LeverPeer holds information about a Lever RPC source or destination process.
type LeverPeer struct {
	Environment string
	Service     string
	Resource    string
}

// ParseLeverURL parses a URL into a LeverPeer. Formats accepted are
//
// Absolute:
//     https://<host>[:<port>]/<service>[/<resource>]
//
// or
//
// Relative (within same environment):
//     /<service>[/<resource>]
func ParseLeverURL(urlPeer string) (*LeverPeer, error) {
	parsed, err := url.Parse(urlPeer)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("Invalid scheme %v", parsed.Scheme)
	}
	serviceAndResource := parsed.EscapedPath()
	if serviceAndResource != "" && serviceAndResource[0] == '/' {
		serviceAndResource = serviceAndResource[1:]
	}
	firstSep := strings.Index(serviceAndResource, "/")
	var service string
	var resource string
	if firstSep == -1 {
		// Resource not mentioned.
		service = serviceAndResource
		resource = ""
	} else {
		service = serviceAndResource[:firstSep]
		resource = serviceAndResource[firstSep+1:]
	}
	return &LeverPeer{
		Environment: parsed.Host,
		Service:     service,
		Resource:    resource,
	}, nil
}

// String returns the URL string representation of the LeverPeer.
// If Environment is "" then the URL will be relative.
func (peer *LeverPeer) String() string {
	if peer.Environment == "" {
		return fmt.Sprintf("/%s/%s", peer.Service, peer.Resource)
	}
	return fmt.Sprintf(
		"https://%s/%s/%s", peer.Environment, peer.Service, peer.Resource)
}

type leverSrcKey struct{}

// NewLeverSrcContext creates a new context with Lever RPC source information
// attached.
func NewLeverSrcContext(ctx context.Context, src *LeverPeer) context.Context {
	return context.WithValue(ctx, leverSrcKey{}, src)
}

// LeverSrcFromContext returns the Lever RPC source information from ctx, if
// any.
func LeverSrcFromContext(ctx context.Context) (src *LeverPeer, ok bool) {
	src, ok = ctx.Value(leverSrcKey{}).(*LeverPeer)
	return
}

type leverDestKey struct{}

// NewLeverDestContext creates a new context with Lever RPC destination
// information attached.
func NewLeverDestContext(ctx context.Context, dest *LeverPeer) context.Context {
	return context.WithValue(ctx, leverDestKey{}, dest)
}

// LeverDestFromContext returns the Lever RPC destination information from ctx,
// if any.
func LeverDestFromContext(ctx context.Context) (dest *LeverPeer, ok bool) {
	dest, ok = ctx.Value(leverDestKey{}).(*LeverPeer)
	return
}
