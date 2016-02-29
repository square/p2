package memorystore

import (
	"fmt"

	"github.com/square/p2/pkg/store"
)

// MemoryStore implements the main P2 Store interface that stores all data in local
// memory. This is primarily targeted to unit tests to test higher-level components
// without having to spin up a Consul instance required for ConsulStore.
type MemoryStore struct {
	// TODO: finish me
}

func (s *MemoryStore) Reality() store.PodStore                                  { return nil }
func (s *MemoryStore) Intent() store.PodStore                                   { return nil }
func (s *MemoryStore) Health() store.HealthStore                                { return nil }
func (s *MemoryStore) ReplicationControllers() store.ReplicationControllerStore { return nil }
func (s *MemoryStore) RollingUpdates() store.RollingUpdateStore                 { return nil }

func (s *MemoryStore) NewLockManager(name string) (store.LockManager, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *MemoryStore) NewSessionLockManager(
	name string,
	session string,
) (store.LockManager, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *MemoryStore) Ping() error {
	return fmt.Errorf("not implemented")
}

// New creates a new MemoryStore.
func New() (*MemoryStore, error) {
	return new(MemoryStore), nil
}

var _ store.Store = (*MemoryStore)(nil)
