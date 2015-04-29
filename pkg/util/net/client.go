package net

import (
	"net/http"
)

// HeaderTransport provides an http.RoundTripper that wraps around another
// RoundTripper, adding all the key-value pairs in Extras as headers on each
// request.
type headerTransport struct {
	inner  http.RoundTripper
	extras map[string]string
}

func (ht headerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	for k, v := range ht.extras {
		r.Header.Set(k, v)
	}
	return ht.inner.RoundTrip(r)
}

// NewHeaderClient returns an http.Client. It has the same behavior as
// DefaultClient, except that the key-value pairs in extras are added as headers
// on every request.
func NewHeaderClient(extras map[string]string, rt http.RoundTripper) *http.Client {
	if len(extras) == 0 {
		return http.DefaultClient
	}

	return &http.Client{
		Transport: headerTransport{
			inner:  rt,
			extras: extras,
		},
	}
}
