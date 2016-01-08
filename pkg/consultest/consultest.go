// The consultest package contains common routines for setting up a live Consul server for
// use in unit tests. It uses Consul's TestServer from its "testutil", which works well on
// its own for testing inside a single package, but it needs tweaking to be usable from
// within P2's testing environment.
package consultest

import (
	"io/ioutil"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/testutil"
)

// Fixture sets up a test Consul server and provides the client configuration for
// accessing it.
type Fixture struct {
	Server   *testutil.TestServer
	Client   *api.Client
	T        *testing.T
	HTTPPort int
}

// getPorts returns a list of `count` number of unused TCP ports.
func getPorts(t *testing.T, count int) []int {
	ports := make([]int, count)
	for i := 0; i < count; i++ {
		l, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		_, portStr, err := net.SplitHostPort(l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		ports[i], err = strconv.Atoi(portStr)
		if err != nil {
			t.Fatal(err)
		}
	}
	return ports
}

// NewFixture creates a new testing instance of Consul.
func NewFixture(t *testing.T) Fixture {
	// Every unit test that starts Consul has a ~1 second startup time before the test can
	// run, so testing a full package tends to take a while. If the -short flag is
	// present, just skip the test.
	if testing.Short() {
		t.Skip("skipping test dependent on consul because of short mode")
	}
	// If the "consul" command isn't on the user's path, NewTestServer() will skip the
	// test. We'd rather make sure that the test actually runs, so this "skip" gets
	// converted into an error.
	defer func() {
		if t.Skipped() {
			t.Error("failing skipped test")
		}
	}()

	var httpPort int
	server := testutil.NewTestServerConfig(t, func(c *testutil.TestServerConfig) {
		// Consul's normal debugging output is very noisy and always starts up with a lot
		// of error messages.
		c.Stdout = ioutil.Discard
		c.Stderr = ioutil.Discard

		// Get random ports to allow multiple packages to execute Consul tests
		// concurrently.
		ports := getPorts(t, 6)
		c.Ports = &testutil.TestPortConfig{
			DNS:     ports[0],
			HTTP:    ports[1],
			RPC:     ports[2],
			SerfLan: ports[3],
			SerfWan: ports[4],
			Server:  ports[5],
		}
		httpPort = c.Ports.HTTP
	})
	client, err := api.NewClient(&api.Config{
		Address: server.HTTPAddr,
	})
	if err != nil {
		server.Stop()
		t.Fatal(err)
	}
	t.Log("starting Consul server with port:", httpPort)
	return Fixture{
		Server:   server,
		Client:   client,
		T:        t,
		HTTPPort: httpPort,
	}
}

// Stop will stop the Consul test server and deallocate any other resources in the testing
// fixture. It should always be called at the end of a unit test.
func (f Fixture) Stop() {
	f.T.Log("stopping Consul server with port:", f.HTTPPort)
	f.Server.Stop()
	time.Sleep(50 * time.Millisecond)
}

// StopOnPanic will stop the Consul test server, but only if it's catching a panic. This
// method is meant to be used whenever a test fixture is created in a method that creates
// a fixture and returns it.
func (f Fixture) StopOnPanic() {
	if r := recover(); r != nil {
		f.Server.Stop()
		panic(r)
	}
}

func (f Fixture) GetKV(key string) []byte {
	p, _, err := f.Client.KV().Get(key, nil)
	if err != nil {
		f.T.Fatal(err)
	}
	if p == nil {
		f.T.Fatalf("key does not exist: %s", key)
	}
	return p.Value
}

func (f Fixture) SetKV(key string, val []byte) {
	_, err := f.Client.KV().Put(&api.KVPair{
		Key:   key,
		Value: val,
	}, nil)
	if err != nil {
		f.T.Fatal(err)
	}
}
