package statusstoretest

import (
	"fmt"
	"sync"

	"github.com/square/p2/pkg/kp/statusstore"
)

// Implementation of the statusstore.Store interface that can be used for unit
// testing
type FakeStatusStore struct {
	Statuses map[StatusIdentifier]statusstore.Status
	mu       sync.Mutex
}

var _ statusstore.Store = &FakeStatusStore{}

// Just a convenient index into the status map that models the interface arguments
type StatusIdentifier struct {
	resourceType statusstore.ResourceType
	resourceID   statusstore.ResourceID
	namespace    statusstore.Namespace
}

func (s StatusIdentifier) String() string {
	return fmt.Sprintf("%s/%s/%s", s.resourceType, s.resourceID, s.namespace)
}

func NewFake() *FakeStatusStore {
	return &FakeStatusStore{
		Statuses: make(map[StatusIdentifier]statusstore.Status),
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
	return nil
}

func (s *FakeStatusStore) GetStatus(
	t statusstore.ResourceType,
	id statusstore.ResourceID,
	namespace statusstore.Namespace,
) (statusstore.Status, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	identifier := StatusIdentifier{t, id, namespace}
	status, ok := s.Statuses[identifier]
	if !ok {
		return statusstore.Status{}, statusstore.NoStatusError{identifier.String()}
	}

	return status, nil
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
