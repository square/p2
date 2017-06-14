// +build !race

package rcstore

import (
	"context"
	"testing"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"

	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestCreateTxn(t *testing.T) {
	fixture := consulutil.NewFixture(t)

	applicator := labels.NewConsulApplicator(fixture.Client, 0)
	store := NewConsul(fixture.Client, applicator, 0)

	rcLabelsToSet := klabels.Set{
		"some_rc_label": "some_rc_value",
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	rc, err := store.CreateTxn(ctx, testManifest(), klabels.Everything(), nil, rcLabelsToSet)
	if err != nil {
		t.Fatal(err)
	}

	// make sure neither the RC nor its labels were created since we haven't committed the transaction yet
	_, err = store.Get(rc.ID)
	switch {
	case err == nil:
		t.Fatal("expected the RC to not be created before transaction is committed")
	case err == NoReplicationController:
	case err != nil:
		t.Fatalf("unexpected error checking if RC was created (it shouldn't be) %s", err)
	}

	rcLabels, err := applicator.GetLabels(labels.RC, rc.ID.String())
	if err != nil {
		t.Fatalf("unexpected error ensuring RC labels don't exist before committing the transaction: %s", err)
	}

	if len(rcLabels.Labels) != 0 {
		t.Fatalf("expected there to be no labels for the RC before the transaction is committed")
	}

	err = transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err != nil {
		t.Fatalf("unexpected error committing RC creation transaction: %s", err)
	}

	_, err = store.Get(rc.ID)
	if err != nil {
		t.Fatalf("unexpected error fetching RC after committing transaction: %s", err)
	}

	rcLabels, err = applicator.GetLabels(labels.RC, rc.ID.String())
	if err != nil {
		t.Fatalf("unexpected error ensuring RC labels were created after committing the transaction: %s", err)
	}

	if len(rcLabels.Labels) != 2 {
		t.Errorf("expected there to be only %q and %q labels but found %d total: %s", PodIDLabel, "some_rc_label", len(rcLabels.Labels), rcLabels.Labels)
	}

	podIDLabel := rcLabels.Labels[PodIDLabel]
	if podIDLabel != testManifest().ID().String() {
		t.Errorf("expected value for label key %q to be %q but was %q", PodIDLabel, testManifest().ID().String(), podIDLabel)
	}

	extraLabel := rcLabels.Labels["some_rc_label"]
	if extraLabel != "some_rc_value" {
		t.Errorf("expected value for label key %q to be %q but was %q", "some_rc_label", "some_rc_value", extraLabel)
	}
}

func testManifest() manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID("some_pod_id")
	return builder.GetManifest()
}
