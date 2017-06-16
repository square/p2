package podstatus

import (
	"context"
	"testing"

	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/statusstoretest"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
)

func TestSetAndGetStatus(t *testing.T) {
	store := newFixture()

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

	podKey := types.NewPodUUID()
	err := store.Set(podKey, testStatus)
	if err != nil {
		t.Fatalf("Unexpected error setting status: %s", err)
	}

	status, _, err := store.Get(podKey)
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

func TestDelete(t *testing.T) {
	store := newFixture()
	testStatus := PodStatus{
		PodStatus: PodLaunched,
	}

	// Put a value in the store
	podKey := types.NewPodUUID()
	err := store.Set(podKey, testStatus)
	if err != nil {
		t.Fatalf("Unexpected error setting status: %s", err)
	}

	// Get the value out to confirm it's there
	status, _, err := store.Get(podKey)
	if err != nil {
		t.Fatalf("Unexpected error getting status: %s", err)
	}

	if status.PodStatus != PodLaunched {
		t.Fatalf("expected pod state to be %q but was %q", PodLaunched, status.PodStatus)
	}

	// Now delete it
	err = store.Delete(podKey)
	if err != nil {
		t.Fatalf("error deleting pod status: %s", err)
	}

	_, _, err = store.Get(podKey)
	if err == nil {
		t.Fatal("expected an error fetching a deleted pod status")
	}

	if !statusstore.IsNoStatus(err) {
		t.Errorf("expected error to be NoStatus but was %s", err)
	}
}

func TestMutateStatusNewKey(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	consulStore := statusstore.NewConsul(fixture.Client)
	podStore := NewConsul(consulStore, "test_namespace")

	key := types.NewPodUUID()
	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err := podStore.MutateStatus(ctx, key, func(p PodStatus) (PodStatus, error) {
		p.PodStatus = PodLaunched
		return p, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	// Now try to get it and confirm the status was set
	status, _, err := podStore.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if status.PodStatus != PodLaunched {
		t.Errorf("Expected pod status to be set to '%s' but was '%s'", PodLaunched, status.PodStatus)
	}
}

func TestMutateStatusExistingKey(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	consulStore := statusstore.NewConsul(fixture.Client)
	podStore := NewConsul(consulStore, "test_namespace")

	key := types.NewPodUUID()
	processStatus := ProcessStatus{
		EntryPoint:   "echo_service",
		LaunchableID: "some_launchable",
		LastExit:     nil,
	}
	err := podStore.Set(key, PodStatus{
		ProcessStatuses: []ProcessStatus{
			processStatus,
		},
	})
	if err != nil {
		t.Fatalf("Unable to set up test with an existing key: %s", err)
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err = podStore.MutateStatus(ctx, key, func(p PodStatus) (PodStatus, error) {
		p.PodStatus = PodLaunched
		return p, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	// Now try to get it and confirm the status was set
	status, _, err := podStore.Get(key)
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

func TestList(t *testing.T) {
	store := newFixture()

	key := types.NewPodUUID()
	err := store.Set(key, PodStatus{
		PodStatus: PodLaunched,
	})
	if err != nil {
		t.Fatalf("Unable to set up test with an existing key: %s", err)
	}

	allStatus, err := store.List()
	if err != nil {
		t.Fatalf("unexpected error listing pod status: %s", err)
	}

	if len(allStatus) != 1 {
		t.Fatalf("expected one status record but there were %d", len(allStatus))
	}

	val, ok := allStatus[key]
	if !ok {
		t.Fatalf("expected a record for pod %s but there wasn't", key)
	}

	if val.PodStatus != PodLaunched {
		t.Errorf("expected pod status of status record to be %q but was %q", PodLaunched, val.PodStatus)
	}
}

func newFixture() *ConsulStore {
	return &ConsulStore{
		statusStore: statusstoretest.NewFake(),
		namespace:   "test_namespace",
	}
}
