package rcstatus

import (
	"context"
	"testing"

	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"

	"github.com/hashicorp/consul/api"
)

type testHelper struct {
	consulFixture consulutil.Fixture
	store         ConsulStore
	status        Status
	id            fields.ID
}

func (th *testHelper) stop() {
	th.consulFixture.Stop()
}

func initTestHelper(t *testing.T) testHelper {
	consulFixture := consulutil.NewFixture(t)
	store := NewConsul(statusstore.NewConsul(consulFixture.Client), "test")
	status := Status{
		NodeTransfer: &NodeTransfer{
			OldNode: types.NodeName("old.123"),
			NewNode: types.NodeName("new.456"),
		},
	}
	id := fields.ID("rc_id")

	return testHelper{
		consulFixture: consulFixture,
		store:         store,
		status:        status,
		id:            id,
	}
}

func TestSetAndGetStatus(t *testing.T) {
	th := initTestHelper(t)
	defer th.stop()

	_, _, err := th.store.Get(th.id)
	if !statusstore.IsNoStatus(err) {
		t.Fatalf("Expected no status error, got: %s", err)
	}

	err = th.store.Set(th.id, th.status)
	if err != nil {
		t.Fatalf("Unexpected error setting status: %s", err)
	}

	status, _, err := th.store.Get(th.id)
	if err != nil {
		t.Fatalf("Unexpected error getting status: %s", err)
	}

	if *(status.NodeTransfer) != *(th.status.NodeTransfer) {
		t.Fatalf("NodeTransfer was %v, wanted %v", status.NodeTransfer, th.status.NodeTransfer)
	}
}

func TestCASTxn(t *testing.T) {
	th := initTestHelper(t)
	defer th.stop()

	// Test check and set new status
	status, queryMeta, err := testCASTxnHelper(th, 0)
	if err != nil {
		t.Fatal(err)
	}

	if *(status.NodeTransfer) != *(th.status.NodeTransfer) {
		t.Fatalf("NodeTransfer was %v, wanted %v", status.NodeTransfer, th.status.NodeTransfer)
	}

	// Test mutate existing status
	mutatedNode := types.NodeName("new.789")
	th.status.NodeTransfer.NewNode = mutatedNode

	status, _, err = testCASTxnHelper(th, queryMeta.LastIndex)
	if err != nil {
		t.Fatal(err)
	}

	if status.NodeTransfer.NewNode != mutatedNode {
		t.Fatalf("NewNode was %v, wanted %v", status.NodeTransfer.NewNode, mutatedNode)
	}

	// Test bad modify index
	_, _, err = testCASTxnHelper(th, 1000)
	if err == nil {
		t.Fatalf("Expected error but err was nil")
	}
}

func TestDelete(t *testing.T) {
	th := initTestHelper(t)
	defer th.stop()

	// Put a value in the store
	err := th.store.Set(th.id, th.status)
	if err != nil {
		t.Fatalf("Unexpected error setting status: %s", err)
	}

	// Get the value out to confirm it's there
	status, _, err := th.store.Get(th.id)
	if err != nil {
		t.Fatalf("Unexpected error getting status: %s", err)
	}

	if *(status.NodeTransfer) != *(th.status.NodeTransfer) {
		t.Fatalf("Expected status.NodeTransfer to be %v, but was %v", th.status.NodeTransfer, status.NodeTransfer)
	}

	// Now delete it
	err = th.store.Delete(th.id)
	if err != nil {
		t.Fatalf("Error deleting rc status: %s", err)
	}

	_, _, err = th.store.Get(th.id)
	if err == nil {
		t.Fatal("Expected an error fetching a deleted status")
	}

	if !statusstore.IsNoStatus(err) {
		t.Errorf("Expected error to be NoStatus but was %s", err)
	}
}

func testCASTxnHelper(th testHelper, modifyIndex uint64) (*Status, *api.QueryMeta, error) {
	writeCtx, writeCancel := transaction.New(context.Background())
	defer writeCancel()

	err := th.store.CASTxn(writeCtx, th.id, modifyIndex, th.status)
	if err != nil {
		return nil, nil, err
	}

	err = transaction.MustCommit(writeCtx, th.consulFixture.Client.KV())
	if err != nil {
		return nil, nil, err
	}

	status, queryMeta, err := th.store.Get(th.id)
	if err != nil {
		return nil, nil, err
	}

	return &status, queryMeta, nil
}
