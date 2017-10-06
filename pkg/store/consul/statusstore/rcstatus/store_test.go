package rcstatus

import (
	"testing"

	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/statusstoretest"
	"github.com/square/p2/pkg/types"
)

func TestSetAndGetStatus(t *testing.T) {
	store := newFixture()
	testStatus := RCStatus{
		NodeTransfer: NodeTransfer{
			OldNode: types.NodeName("old.123"),
			NewNode: types.NodeName("new.456"),
		},
	}

	rcID := fields.ID("rc_id")
	err := store.Set(rcID, testStatus)
	if err != nil {
		t.Fatalf("Unexpected error setting rc status: %s", err)
	}

	status, _, err := store.Get(rcID)
	if err != nil {
		t.Fatalf("Unexpected error getting status: %s", err)
	}

	if status.NodeTransfer.OldNode == "" || status.NodeTransfer.NewNode == "" {
		t.Fatalf("Expected oldNode and newNode to be set but they were not")
	}

	if status.NodeTransfer.OldNode != testStatus.NodeTransfer.OldNode {
		t.Fatalf("Expected oldNode to be %s, was %s", testStatus.NodeTransfer.OldNode, status.NodeTransfer.OldNode)
	}

	if status.NodeTransfer.NewNode != testStatus.NodeTransfer.NewNode {
		t.Fatalf("Expected newNode to be %s, was %s", testStatus.NodeTransfer.NewNode, status.NodeTransfer.NewNode)
	}
}

func TestDelete(t *testing.T) {
	store := newFixture()
	testStatus := RCStatus{
		NodeTransfer: NodeTransfer{
			OldNode: types.NodeName("old.123"),
			NewNode: types.NodeName("new.456"),
		},
	}

	// Put a value in the store
	rcID := fields.ID("rc_id")
	err := store.Set(rcID, testStatus)
	if err != nil {
		t.Fatalf("Unexpected error setting status: %s", err)
	}

	// Get the value out to confirm it's there
	status, _, err := store.Get(rcID)
	if err != nil {
		t.Fatalf("Unexpected error getting status: %s", err)
	}

	if status.NodeTransfer != testStatus.NodeTransfer {
		t.Fatalf("Expected status.NodeTransfer to be %v, but was %v", testStatus.NodeTransfer, status.NodeTransfer)
	}

	// Now delete it
	err = store.Delete(rcID)
	if err != nil {
		t.Fatalf("Error deleting rc status: %s", err)
	}

	_, _, err = store.Get(rcID)
	if err == nil {
		t.Fatal("Expected an error fetching a deleted status")
	}

	if !statusstore.IsNoStatus(err) {
		t.Errorf("Expected error to be NoStatus but was %s", err)
	}
}

func newFixture() *ConsulStore {
	return &ConsulStore{
		statusStore: statusstoretest.NewFake(),
		namespace:   "test_namespace",
	}
}
