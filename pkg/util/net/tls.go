package net

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/square/p2/pkg/util"
)

// getTLSConfig constructs a tls.Config that uses keys/certificates in the given files.
func GetTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	var certs []tls.Certificate
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, util.Errorf("TLS client requires both cert file and key file")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, util.Errorf("Could not load keypair: %s", err)
		}
		certs = append(certs, cert)
	}

	var cas *x509.CertPool
	if caFile != "" {
		cas = x509.NewCertPool()
		caBytes, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		ok := cas.AppendCertsFromPEM(caBytes)
		if !ok {
			return nil, util.Errorf("Could not parse certificate file: %s", caFile)
		}
	}

	tlsConfig := &tls.Config{
		Certificates: certs,
		ClientCAs:    cas,
		RootCAs:      cas,
	}
	return tlsConfig, nil
}
