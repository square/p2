// Package flags provides frequently used kingpin flags for command-line tools
// that connect to Consul.
package flags

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/store/consul"
	netutil "github.com/square/p2/pkg/util/net"
	"gopkg.in/alecthomas/kingpin.v2"
)

func ParseWithConsulOptions() (string, consul.Options, labels.ApplicatorWithoutWatches) {
	consulURL := kingpin.Flag("consul", "The hostname and port of a consul agent in the p2 cluster. Defaults to 0.0.0.0:8500.").String()
	httpApplicatorURL := kingpin.Flag("http-applicator-url", "The URL of an labels.httpApplicator target, including the protocol and port. For example, https://consul-server.io:9999").URL()
	token := kingpin.Flag("token", "The consul ACL token to use. Empty by default.").String()
	tokenFile := kingpin.Flag("token-file", "The file containing the Consul ACL token").ExistingFile()
	headers := kingpin.Flag("header", "An HTTP header to add to requests, in KEY=VALUE form. Can be specified multiple times.").StringMap()
	https := kingpin.Flag("https", "Use HTTPS").Bool()
	wait := kingpin.Flag("wait", "Maximum duration for Consul watches, before resetting and starting again.").Default("30s").Duration()
	caFile := kingpin.Flag("tls-ca-file", "File containing the x509 PEM-encoded CA ").ExistingFile()
	keyFile := kingpin.Flag("tls-key-file", "File containing the x509 PEM-encoded private key").ExistingFile()
	certFile := kingpin.Flag("tls-cert-file", "File containing the x509 PEM-encoded public key certificate").ExistingFile()

	cmd := kingpin.Parse()

	if *tokenFile != "" {
		tokenBytes, err := ioutil.ReadFile(*tokenFile)
		if err != nil {
			log.Fatalln(err)
		}
		*token = strings.TrimSpace(string(tokenBytes))
	}
	var transport http.RoundTripper
	if *caFile != "" || *keyFile != "" || *certFile != "" {
		tlsConfig, err := netutil.GetTLSConfig(*certFile, *keyFile, *caFile)
		if err != nil {
			log.Fatalln(err)
		}

		transport = &http.Transport{
			TLSClientConfig: tlsConfig,
			// same dialer as http.DefaultTransport
			Dial: (&net.Dialer{
				Timeout:   http.DefaultClient.Timeout,
				KeepAlive: http.DefaultClient.Timeout,
			}).Dial,
		}
	} else {
		transport = http.DefaultTransport
	}
	httpClient := netutil.NewHeaderClient(*headers, transport)

	consulOpts := consul.Options{
		Address:  *consulURL,
		Token:    *token,
		Client:   httpClient,
		HTTPS:    *https,
		WaitTime: *wait,
	}

	var applicator labels.ApplicatorWithoutWatches
	var err error
	if *httpApplicatorURL != nil {
		applicator, err = labels.NewHTTPApplicator(httpClient, *httpApplicatorURL)
		if err != nil {
			log.Fatalln(err)
		}
	} else {
		jitterWindow := 0 * time.Second // we don't initiate watches in CLIs so this doesn't matter
		applicator = labels.NewConsulApplicator(consul.NewConsulClient(consulOpts), 0, jitterWindow)
	}
	return cmd, consulOpts, applicator
}
