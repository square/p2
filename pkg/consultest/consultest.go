// The consultest package contains common routines for setting up a live Consul server for
// use in unit tests. The server runs within the test process and uses an isolated
// in-memory data store.
package consultest

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/command/agent"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/consul"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/testutil"
)

// Fixture sets up a test Consul server and provides the client configuration for
// accessing it.
type Fixture struct {
	Agent    *agent.Agent
	Servers  []*agent.HTTPServer
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
	ports := getPorts(t, 6)

	config := agent.DevConfig()
	config.BindAddr = "127.0.0.1"
	config.Bootstrap = true
	config.DisableCoordinates = true
	config.NodeName = "testnode"
	config.Ports = agent.PortConfig{
		DNS:     ports[0],
		HTTP:    ports[1],
		RPC:     ports[2],
		SerfLan: ports[3],
		SerfWan: ports[4],
		Server:  ports[5],
	}
	config.ConsulConfig = consul.DefaultConfig()
	// Set low timeouts so the agent will become the leader quickly.
	config.ConsulConfig.RaftConfig.LeaderLeaseTimeout = 10 * time.Millisecond
	config.ConsulConfig.RaftConfig.HeartbeatTimeout = 20 * time.Millisecond
	config.ConsulConfig.RaftConfig.ElectionTimeout = 20 * time.Millisecond
	// We would rather start as the leader without requiring an election, but Consul
	// contains some atomicity violations that are exacerbated by this option.
	//
	// config.ConsulConfig.RaftConfig.StartAsLeader = true

	a, err := agent.Create(config, os.Stdout)
	if err != nil {
		t.Fatal("creating Consul agent:", err)
	}
	servers, err := agent.NewHTTPServers(a, config, os.Stdout)
	if err != nil {
		t.Fatal("creating Consul HTTP server:", err)
	}
	client, err := api.NewClient(&api.Config{
		Address: fmt.Sprintf("%s:%d", config.BindAddr, config.Ports.HTTP),
	})
	if err != nil {
		t.Fatal("creating Consul client:", err)
	}

	testutil.WaitForLeader(t, a.RPC, config.Datacenter)

	t.Log("starting Consul server with port:", config.Ports.HTTP)
	return Fixture{
		Agent:    a,
		Servers:  servers,
		Client:   client,
		T:        t,
		HTTPPort: config.Ports.HTTP,
	}
}

// Stop will stop the Consul test server and deallocate any other resources in the testing
// fixture. It should always be called at the end of a unit test.
func (f Fixture) Stop() {
	f.T.Log("stopping Consul server with port:", f.HTTPPort)
	for _, s := range f.Servers {
		s.Shutdown()
	}
	_ = f.Agent.Shutdown()
	time.Sleep(50 * time.Millisecond)
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
