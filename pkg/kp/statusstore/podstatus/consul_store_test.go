package podstatus

import (
	"testing"

	"github.com/square/p2/pkg/kp/statusstore/statusstoretest"
	"github.com/square/p2/pkg/store"
)

func TestSetAndGetStatus(t *testing.T) {
	consulStore := newFixture()

	processStatus := ProcessStatus{
		EntryPoint:   "echo_service",
		LaunchableID: "some_launchable",
		LastExit:     nil,
	}
	testStatus := PodStatus{
		ProcessStatuses: []ProcessStatus{
			processStatus,
		},
	}

	podKey := store.NewPodUUID()
	err := consulStore.Set(podKey, testStatus)
	if err != nil {
		t.Fatalf("Unexpected error setting status: %s", err)
	}

	status, _, err := consulStore.Get(podKey)
	if err != nil {
		t.Fatalf("Unexpected error getting status: %s", err)
	}

	if len(status.ProcessStatuses) != 1 {
		t.Fatalf("Expected one service status entry, but there were %d", len(status.ProcessStatuses))
	}

	if status.ProcessStatuses[0] != processStatus {
		t.Errorf("Status entry expected to be '%+v', was %+v", processStatus, status.ProcessStatuses[0])
	}
}

func TestMutateStatusNewKey(t *testing.T) {
	consulStore := newFixture()

	key := store.NewPodUUID()
	err := consulStore.MutateStatus(key, func(p PodStatus) (PodStatus, error) {
		p.PodStatus = PodLaunched
		return p, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Now try to get it and confirm the status was set
	status, _, err := consulStore.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if status.PodStatus != PodLaunched {
		t.Errorf("Expected pod status to be set to '%s' but was '%s'", PodLaunched, status.PodStatus)
	}
}

func TestMutateStatusExistingKey(t *testing.T) {
	consulStore := newFixture()

	key := store.NewPodUUID()
	processStatus := ProcessStatus{
		EntryPoint:   "echo_service",
		LaunchableID: "some_launchable",
		LastExit:     nil,
	}
	err := consulStore.Set(key, PodStatus{
		ProcessStatuses: []ProcessStatus{
			processStatus,
		},
	})
	if err != nil {
		t.Fatalf("Unable to set up test with an existing key: %s", err)
	}

	err = consulStore.MutateStatus(key, func(p PodStatus) (PodStatus, error) {
		p.PodStatus = PodLaunched
		return p, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Now try to get it and confirm the status was set
	status, _, err := consulStore.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if status.PodStatus != PodLaunched {
		t.Errorf("Expected pod status to be set to '%s' but was '%s'", PodLaunched, status.PodStatus)
	}

	if len(status.ProcessStatuses) != 1 {
		t.Error("ProcessStatus field didn't go untouched when mutating PodStatus")
	}
}

func newFixture() *consulStore {
	return &consulStore{
		statusStore: statusstoretest.NewFake(),
		namespace:   "test_namespace",
	}
}
