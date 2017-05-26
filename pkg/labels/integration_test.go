// +build !race

package labels

import (
	"context"
	"testing"

	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"
)

func TestRemoveAllLabelsTxn(t *testing.T) {
	fixture := consulutil.NewFixture(t)

	applicator := NewConsulApplicator(fixture.Client, 0)

	// set some labels
	err := applicator.SetLabels(RU, "some_id", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()

	err = applicator.RemoveAllLabelsTxn(ctx, RU, "some_id")
	if err != nil {
		t.Fatal(err)
	}

	// confirm that nothing was actually removed yet because transaction wasn't committed
	labels, err := applicator.GetLabels(RU, "some_id")
	if err != nil {
		t.Fatalf("shouldn't have errored fetching labels we didn't delete yet: %s", err)
	}
	if len(labels.Labels) == 0 {
		t.Fatal("no labels found when we haven't committed transaction for removal yet")
	}

	err = transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err != nil {
		t.Fatalf("error committing transaction: %s", err)
	}

	labels, err = applicator.GetLabels(RU, "some_id")
	if err != nil {
		t.Fatalf("unexpected error checking for labels: %s", err)
	}

	if len(labels.Labels) != 0 {
		t.Errorf("labels were not deleted as expected, found %s", labels.Labels)
	}
}
