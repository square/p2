// Package flags provides frequently used kingpin flags for command-line tools
// that connect to Consul.
package flags

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/util/net"
	"gopkg.in/alecthomas/kingpin.v2"
)

func ParseWithConsulOptions() (string, kp.Options) {
	url := kingpin.Flag("consul", "The hostname and port of a consul agent in the p2 cluster. Defaults to 0.0.0.0:8500.").String()
	token := kingpin.Flag("token", "The consul ACL token to use. Empty by default.").String()
	tokenFile := kingpin.Flag("token-file", "The file containing the Consul ACL token").ExistingFile()
	headers := kingpin.Flag("header", "An HTTP header to add to requests, in KEY=VALUE form. Can be specified multiple times.").StringMap()
	https := kingpin.Flag("https", "Use HTTPS").Bool()
	wait := kingpin.Flag("wait", "Maximum duration for Consul watches, before resetting and starting again.").Default("30s").Duration()

	cmd := kingpin.Parse()

	if *tokenFile != "" {
		tokenBytes, err := ioutil.ReadFile(*tokenFile)
		if err != nil {
			log.Fatalln(err)
		}
		*token = string(tokenBytes)
	}
	return cmd, kp.Options{
		Address:  *url,
		Token:    *token,
		Client:   net.NewHeaderClient(*headers, http.DefaultTransport),
		HTTPS:    *https,
		WaitTime: *wait,
	}
}
