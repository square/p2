package kp

import (
	"fmt"
	"net/http"
)

// Go version of http status check
func HttpStatusCheck(node string, port int) (*http.Response, error) {
	url := fmt.Sprintf("http://%s:%d/_status", node, port)
	return http.Get(url)
}

func HttpsStatusCheck(node string, port int) (*http.Response, error) {
	url := fmt.Sprintf("https://%s:%d/_status", node, port)
	return http.Get(url)
}
