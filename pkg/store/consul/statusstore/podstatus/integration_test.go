// +build !race

package podstatus

import (
	"context"
	"testing"

	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
)

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
	err = transaction.MustCommit(ctx, fixture.Client.KV())
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
	err = transaction.MustCommit(ctx, fixture.Client.KV())
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
