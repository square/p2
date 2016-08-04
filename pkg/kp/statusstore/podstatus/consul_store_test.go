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

	status, err := store.Get(podKey)
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

func newFixture() *consulStore {
	return &consulStore{
		statusStore: statusstoretest.NewFake(),
		namespace:   "test_namespace",
	}
}
