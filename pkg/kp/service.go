package kp

import (
	"fmt"
	"net/http"
	"os"

	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/pods"
)

var (
	// prints the HTTP response on stderr, while exiting 0 if the status code is 200
	httpStatusCheck  = `if [[ $(curl http://%s:%d/_status -s -o /dev/stderr -w "%%{http_code}") == "200" ]] ; then exit 0 ; else exit 2; fi`
	httpsStatusCheck = `if [[ $(curl https://%s:%d/_status -s -o /dev/stderr -w "%%{http_code}" --cacert '%s') == "200" ]] ; then exit 0 ; else exit 2; fi`

	// Defines how frequently the service should be checked
	checkInterval = "5s"
)

// RegisterService creates a consul service for the given pod manifest. If the
// manifest specifies a status port, the resulting consul service will also
// include a health check for that port.
func (c consulStore) RegisterService(manifest pods.Manifest, caPath string) error {
	podService := &api.AgentServiceRegistration{
		Name: manifest.ID(),
	}

	if manifest.StatusPort != 0 {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}
		podService.Port = manifest.StatusPort
		podService.Check = &api.AgentServiceCheck{
			Interval: checkInterval,
		}
		if manifest.StatusHTTP {
			podService.Check.Script = fmt.Sprintf(
				hostname,
				httpStatusCheck,
				manifest.StatusPort,
			)
		} else {
			podService.Check.Script = fmt.Sprintf(
				hostname,
				httpsStatusCheck,
				manifest.StatusPort,
				caPath,
			)
		}
	}

	return c.client.Agent().ServiceRegister(podService)
}

// Go version of http status check
func HttpStatusCheck(node string, port int) (*http.Response, error) {
	url := fmt.Sprintf("http://%s:%d/_status", node, port)
	return http.Get(url)
}

func HttpsStatusCheck(node string, port int) (*http.Response, error) {
	url := fmt.Sprintf("https://%s:%d/_status", node, port)
	return http.Get(url)
}
