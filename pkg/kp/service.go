package kp

import (
	"fmt"

	"github.com/armon/consul-api"
	"github.com/square/p2/pkg/pods"
)

// RegisterService creates a consul service for the given pod manifest. If the
// manifest specifies a status port, the resulting consul service will also
// include a health check for that port.
func (s *Store) RegisterService(manifest pods.PodManifest) error {
	podService := &consulapi.AgentServiceRegistration{
		Name: manifest.ID(),
	}

	if manifest.StatusPort != 0 {
		podService.Port = manifest.StatusPort
		podService.Check = &consulapi.AgentServiceCheck{
			// prints the HTTP response on stderr, while exiting 0 if the status code is 200
			Script: fmt.Sprintf(`if [[ $(curl https://$(hostname):%v/_status -s -o /dev/stderr -w "%%{http_code}" --cacert ${SECRETS_PATH}/service2service.ca.pem) == "200" ]] ; then exit 0 ; else exit 2; fi`, manifest.StatusPort),
			// magic number alert
			Interval: "5s",
		}
	}

	return s.client.Agent().ServiceRegister(podService)
}
