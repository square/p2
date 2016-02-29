package consulstore

import (
	"fmt"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/store"
)

// ConsulStore implements the main P2 Store interface to store all its data in a Consul
// cluster.
type ConsulStore struct {
	Client *api.Client
	// TODO: finish me
}

type podStore struct {
	store  *ConsulStore
	prefix string
}

func (s *ConsulStore) Reality() store.PodStore {
	// return &podStore{cs, "reality"}
	return nil
}

func (s *ConsulStore) Intent() store.PodStore {
	// return &podStore{cs, "intent"}
	return nil
}

func (s *ConsulStore) Health() store.HealthStore {
	return nil

}
func (s *ConsulStore) ReplicationControllers() store.ReplicationControllerStore {
	return nil
}

func (s *ConsulStore) RollingUpdates() store.RollingUpdateStore {
	return nil
}

func (s *ConsulStore) NewLockManager(name string) (store.LockManager, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *ConsulStore) NewSessionLockManager(
	name string,
	session string,
) (store.LockManager, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *ConsulStore) Ping() error {
	return fmt.Errorf("not implemented")
}

type Options struct {
	// TODO: finish me
}

func New(opt Options) (*ConsulStore, error) {
	return new(ConsulStore), nil
}

var _ store.Store = (*ConsulStore)(nil)
