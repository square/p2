package kp

import (
	"fmt"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/pods"
)

var (
	// prints the HTTP response on stderr, while exiting 0 if the status code is 200
	httpStatusCheck  = `if [[ $(curl http://$(hostname):%v/_status -s -o /dev/stderr -w "%%{http_code}") == "200" ]] ; then exit 0 ; else exit 2; fi`
	httpsStatusCheck = `if [[ $(curl https://$(hostname):%v/_status -s -o /dev/stderr -w "%%{http_code}" --cacert '%s') == "200" ]] ; then exit 0 ; else exit 2; fi`
)

// RegisterService creates a consul service for the given pod manifest. If the
// manifest specifies a status port, the resulting consul service will also
// include a health check for that port.
func (s *Store) RegisterService(manifest pods.Manifest, caPath string) error {
	podService := &api.AgentServiceRegistration{
		Name: manifest.ID(),
	}

	if manifest.StatusPort != 0 {
		podService.Port = manifest.StatusPort
		podService.Check = &api.AgentServiceCheck{
			// magic number alert
			Interval: "5s",
		}
		if manifest.StatusHTTP {
			podService.Check.Script = fmt.Sprintf(httpStatusCheck, manifest.StatusPort)
		} else {
			podService.Check.Script = fmt.Sprintf(httpsStatusCheck, manifest.StatusPort, caPath)
		}
	}

	return s.client.Agent().ServiceRegister(podService)
}
