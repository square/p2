package podstatus

import (
	"testing"

	"github.com/square/p2/pkg/kp/statusstore/statusstoretest"
	"github.com/square/p2/pkg/types"
)

func TestSetAndGetStatus(t *testing.T) {
	store := newFixture()

	serviceStatus := ServiceStatus{
		Name:     "echo_service",
		State:    Running,
		LastExit: nil,
	}
	testStatus := PodStatus{
		ServiceStatus: []ServiceStatus{
			serviceStatus,
		},
	}

	podKey := types.NewPodUUID()
	err := store.Set(podKey, testStatus)
	if err != nil {
		t.Fatalf("Unexpected error setting status: %s", err)
	}

	status, _, err := store.Get(podKey)
	if err != nil {
		t.Fatalf("Unexpected error getting status: %s", err)
	}

	if len(status.ServiceStatus) != 1 {
		t.Fatalf("Expected one service status entry, but there were %d", len(status.ServiceStatus))
	}

	if status.ServiceStatus[0] != serviceStatus {
		t.Errorf("Status entry expected to be '%+v', was %+v", serviceStatus, status.ServiceStatus[0])
	}
}

func TestMutateStatusNewKey(t *testing.T) {
	store := newFixture()

	key := types.NewPodUUID()
	err := store.MutateStatus(key, func(p PodStatus) (PodStatus, error) {
		p.PodStatus = PodLaunched
		return p, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Now try to get it and confirm the status was set
	status, _, err := store.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if status.PodStatus != PodLaunched {
		t.Errorf("Expected pod status to be set to '%s' but was '%s'", PodLaunched, status.PodStatus)
	}
}

func TestMutateStatusExistingKey(t *testing.T) {
	store := newFixture()

	key := types.NewPodUUID()
	serviceStatus := ServiceStatus{
		Name:     "echo_service",
		State:    Running,
		LastExit: nil,
	}
	err := store.Set(key, PodStatus{
		ServiceStatus: []ServiceStatus{
			serviceStatus,
		},
	})
	if err != nil {
		t.Fatalf("Unable to set up test with an existing key: %s", err)
	}

	err = store.MutateStatus(key, func(p PodStatus) (PodStatus, error) {
		p.PodStatus = PodLaunched
		return p, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Now try to get it and confirm the status was set
	status, _, err := store.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if status.PodStatus != PodLaunched {
		t.Errorf("Expected pod status to be set to '%s' but was '%s'", PodLaunched, status.PodStatus)
	}

	if len(status.ServiceStatus) != 1 {
		t.Error("ServiceStatus field didn't go untouched when mutating PodStatus")
	}
}

func newFixture() *consulStore {
	return &consulStore{
		statusStore: statusstoretest.NewFake(),
		namespace:   "test_namespace",
	}
}
