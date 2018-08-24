package socket

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/square/p2/pkg/util"
)

// UnixSocketTransport provides an http.RoundTripper that wraps around another
// RoundTripper, adding needed metadata to each request
type UnixSocketTransport struct {
	// Transport does the actual work of sending the HTTP requests after
	// UnixSocketTransport rewrites certain request metadata
	*http.Transport

	// The name of Consul added as the request host header
	ConsulHost string
}

// RoundTrip implements the http.RoundTripper interface. It modifies the HTTP
// request to inject the necessary request metadata for the request to be
// route the request to the right place. The modified HTTP request is then
// passed to the wrapped *http.Transport (also an implementation of
// RoundTripper).
func (t UnixSocketTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Set the scheme to "http" because we're talking to a unix socket protected
	// with filesystem permissions, so we don't need or want TLS
	r.URL.Scheme = "http"

	// Encode Consul name in the host header for the request to be routed on
	r.Host = t.ConsulHost

	// Perform the actual HTTP request
	return t.Transport.RoundTrip(r)
}

// NewHTTPClient returns an *http.Client which will send all HTTP requests to the unix socket at socketPath,
// and will encode the proper request metadata to make all requests route to Consul
func NewHTTPClient(socketPath, consulHost string) *http.Client {
	return &http.Client{
		Transport: NewTransport(socketPath, consulHost),
		// 6 minutes is slightly higher than the wait time we use on consul watches of
		// 5 minutes. We expect that a response might not come back for up to 5
		// minutes, but we shouldn't wait much longer than that
		Timeout: 6 * time.Minute,
	}
}

func NewTransport(socketPath, consulHost string) http.RoundTripper {
	return UnixSocketTransport{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
				return net.DialUnix("unix", nil, &net.UnixAddr{
					Net:  "unix",
					Name: socketPath,
				})
			},
		},
		ConsulHost: consulHost,
	}
}

func Path(pathEnvVar string) (string, error) {
	path := os.Getenv(pathEnvVar)
	if path == "" {
		return "", util.Errorf("%s is not set", pathEnvVar)
	}

	return path, nil
}
