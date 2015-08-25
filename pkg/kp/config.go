package kp

import (
	"net/http"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
)

type Options struct {
	// The hostname and port of Consul (eg "example.com:8500"), or a unix socket
	// (eg "unix:///var/run/consul.sock"). The empty string defaults to
	// "127.0.0.1:8500".
	Address string
	// Set to true to use HTTPS.
	HTTPS bool
	// The ACL token to pass to Consul.
	Token string
	// If non-nil, this http.Client will be used for Consul communication.
	Client *http.Client
	// If provided, the wait time to be used on queries from this client.
	// See the "wait" parameter:
	// https://consul.io/intro/getting-started/kv.html
	WaitTime time.Duration

	// Feature flag to use the new session-based health manager
	UseSessionHealth bool
}

func NewConsulClient(opts Options) *api.Client {
	conf := api.DefaultConfig()
	if opts.Address != "" {
		conf.Address = opts.Address
	}
	if opts.Client != nil {
		conf.HttpClient = opts.Client
	}
	if opts.HTTPS {
		conf.Scheme = "https"
	}
	conf.Token = opts.Token
	if opts.WaitTime != 0 {
		conf.WaitTime = opts.WaitTime
	}

	// error is always nil
	ret, _ := api.NewClient(conf)
	return ret
}
