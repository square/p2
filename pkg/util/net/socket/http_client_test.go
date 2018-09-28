package socket

import (
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/square/p2/pkg/util"
)

func startUnixHTTPServer(ctx context.Context, socketPath string, handlerFunc http.HandlerFunc) error {
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	return http.Serve(ln, handler{handlerFunc: handlerFunc})
}

type handler struct {
	handlerFunc http.HandlerFunc
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.handlerFunc(w, r)
}

func TestClient(t *testing.T) {
	// Need to use /tmp because TMPDIR ends up giving us a socket path that is
	// greater than the max socket path length
	tempDir, err := ioutil.TempDir("/tmp", "test_http_client")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	socketPath := filepath.Join(tempDir, "test_http.sock")

	client := NewHTTPClient(socketPath, "consul-production", nil)

	ctx, cancel := context.WithCancel(context.Background())

	reqCh := make(chan http.Request)
	defer close(reqCh)
	serverErrCh := make(chan error)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer close(serverErrCh)
		defer wg.Done()
		err := startUnixHTTPServer(ctx, socketPath, func(w http.ResponseWriter, r *http.Request) {
			reqCh <- *r
		})
		if err != nil {
			serverErrCh <- err
		}
	}()

	clientErrCh := make(chan error)
	defer close(clientErrCh)

	wg.Add(1)
	go func() {
		// sigh, give the server some time to start
		time.Sleep(1 * time.Millisecond)

		defer wg.Done()
		resp, err := client.Get("http://whatever/foo/bar")
		if err != nil {
			clientErrCh <- util.Errorf("HTTP request via socket client unexpectedly failed: %s", err)
			return
		}
		defer resp.Body.Close()
	}()

	var req http.Request
	select {
	case req = <-reqCh:
		t.Log("got a request!")
	case err := <-serverErrCh:
		t.Fatalf("got unexpected error running HTTP server: %s", err)
	case err := <-clientErrCh:
		t.Fatalf("got unexpected error making HTTP request: %s", err)
	}
	cancel()

	expectedHost := "consul-production"
	if req.Host != expectedHost {
		t.Errorf("expected request's Host field to be rewritten to %q but was %q", expectedHost, req.Host)
	}
	if req.URL.Path != "/foo/bar" {
		t.Errorf("the request's path was rewritten to %q", req.URL.Path)
	}

	// drain server error channel (we expect it to error when we close the listener)
	for range serverErrCh {
	}

	wg.Wait()
}
