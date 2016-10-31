package statusstoretest

import (
	"fmt"
	"sync"

	"github.com/square/p2/pkg/kp/statusstore"

	"github.com/hashicorp/consul/api"
)

// Implementation of the statusstore.Store interface that can be used for unit
// testing
type FakeStatusStore struct {
	// mu synchronizes access to Statuses and Last Index
	mu sync.Mutex

	Statuses map[StatusIdentifier]statusstore.Status

	// Imitates the ModifyIndex capability of consul, enabling CAS operations
	LastIndex uint64
}

var _ statusstore.Store = &FakeStatusStore{}

// Just a convenient index into the status map that models the interface arguments
type StatusIdentifier struct {
	resourceType statusstore.ResourceType
	resourceID   statusstore.ResourceID
	namespace    statusstore.Namespace
}

func (s StatusIdentifier) String() string {
	return fmt.Sprintf("status/%s/%s/%s", s.resourceType, s.resourceID, s.namespace)
}

func NewFake() *FakeStatusStore {
	return &FakeStatusStore{
		Statuses:  make(map[StatusIdentifier]statusstore.Status),
		LastIndex: 1234, // start above 0 to not allow some false positives on edge cases (e.g. CAS on a non-existing key)
	}
}

func (s *FakeStatusStore) SetStatus(
	t statusstore.ResourceType,
	id statusstore.ResourceID,
	namespace statusstore.Namespace,
	status statusstore.Status,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	identifier := StatusIdentifier{t, id, namespace}
	s.Statuses[identifier] = status
	s.LastIndex++
	return nil
}

func (s *FakeStatusStore) CASStatus(
	t statusstore.ResourceType,
	id statusstore.ResourceID,
	namespace statusstore.Namespace,
	status statusstore.Status,
	modifyIndex uint64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	identifier := StatusIdentifier{t, id, namespace}
	if modifyIndex == 0 {
		// Check that the key doesn't exist which is what consul does https://www.consul.io/docs/agent/http/kv.html
		_, ok := s.Statuses[identifier]
		if ok {
			return statusstore.NewStaleIndex(id.String(), modifyIndex)
		}
	} else if modifyIndex != s.LastIndex {
		return statusstore.NewStaleIndex(id.String(), modifyIndex)
	}

	s.Statuses[identifier] = status
	s.LastIndex++
	return nil
}

func (s *FakeStatusStore) GetStatus(
	t statusstore.ResourceType,
	id statusstore.ResourceID,
	namespace statusstore.Namespace,
) (statusstore.Status, *api.QueryMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	identifier := StatusIdentifier{t, id, namespace}
	status, ok := s.Statuses[identifier]
	if !ok {
		// The behavior of the consul API is to return the last index even on
		// a 404, making it somewhat difficult to do a CAS operation
		// afterward (have to use 0 for the index).
		return statusstore.Status{}, &api.QueryMeta{LastIndex: s.LastIndex}, statusstore.NoStatusError{Key: identifier.String()}
	}

	return status, &api.QueryMeta{LastIndex: s.LastIndex}, nil
}

func (s *FakeStatusStore) DeleteStatus(
	t statusstore.ResourceType,
	id statusstore.ResourceID,
	namespace statusstore.Namespace,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	identifier := StatusIdentifier{t, id, namespace}
	delete(s.Statuses, identifier)
	s.LastIndex++
	return nil
}

func (s *FakeStatusStore) GetAllStatusForResource(
	t statusstore.ResourceType,
	id statusstore.ResourceID,
) (map[statusstore.Namespace]statusstore.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret := make(map[statusstore.Namespace]statusstore.Status)
	for identifier, status := range s.Statuses {
		if identifier.resourceType == t && identifier.resourceID == id {
			ret[identifier.namespace] = status
		}
	}

	return ret, nil
}

func (s *FakeStatusStore) GetAllStatusForResourceType(
	t statusstore.ResourceType,
) (map[statusstore.ResourceID]map[statusstore.Namespace]statusstore.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := make(map[statusstore.ResourceID]map[statusstore.Namespace]statusstore.Status)

	for identifier, status := range s.Statuses {
		if identifier.resourceType == t {
			if ret[identifier.resourceID] == nil {
				ret[identifier.resourceID] = make(map[statusstore.Namespace]statusstore.Status)
			}

			ret[identifier.resourceID][identifier.namespace] = status
		}
	}

	return ret, nil
}
