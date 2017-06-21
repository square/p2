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
	defer fixture.Stop()

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

func TestSetLabelTxn(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := NewConsulApplicator(fixture.Client, 0)

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err := applicator.SetLabelTxn(ctx, RU, "some_id", "some_key", "some_value")
	if err != nil {
		t.Fatal(err)
	}

	// confirm that the label was not set yet because the transaction wasn't committed
	labeled, err := applicator.GetLabels(RU, "some_id")
	if err != nil {
		t.Fatal(err)
	}

	if len(labeled.Labels) != 0 {
		t.Fatal("labels were set before transaction was committed!")
	}

	err = transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	// confirm that the label was now set
	labeled, err = applicator.GetLabels(RU, "some_id")
	if err != nil {
		t.Fatal(err)
	}

	if labeled.Labels["some_key"] != "some_value" {
		t.Errorf("expected value for %q to be %q but was %q", "some_key", "some_value", labeled.Labels["some_key"])
	}
}

func TestSetLabelTxnFailsIfLabelsChange(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := NewConsulApplicator(fixture.Client, 0)

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err := applicator.SetLabelTxn(ctx, RU, "some_id", "some_key", "some_value")
	if err != nil {
		t.Fatal(err)
	}

	// now mutate the same label entry out of band and make sure the
	// transaction fails (which should happen because it is configured as a
	// "cas" operation)
	err = applicator.SetLabel(RU, "some_id", "some_other_key", "some_other_value")
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err == nil {
		t.Fatal("expected the transaction to fail when the labels are changed out from under it")
	}
}

func TestRemoveLabelTxn(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := NewConsulApplicator(fixture.Client, 0)

	// set some labels
	err := applicator.SetLabel(RU, "some_id", "some_key", "some_value")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err = applicator.RemoveLabelTxn(ctx, RU, "some_id", "some_key")
	if err != nil {
		t.Fatal(err)
	}

	// confirm that the label was not removed yet because the transaction wasn't committed
	labeled, err := applicator.GetLabels(RU, "some_id")
	if err != nil {
		t.Fatal(err)
	}

	if labeled.Labels["some_key"] != "some_value" {
		t.Errorf("expected value for %q to be %q but was %q", "some_key", "some_value", labeled.Labels["some_key"])
	}

	err = transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	// confirm that the label was now set
	labeled, err = applicator.GetLabels(RU, "some_id")
	if err != nil {
		t.Fatal(err)
	}

	if len(labeled.Labels) != 0 {
		t.Errorf("labels were not deleted as expected, found %s", labeled.Labels)
	}
}

func TestRemoveLabelTxnFailsIfLabelsChange(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := NewConsulApplicator(fixture.Client, 0)

	// set some labels
	err := applicator.SetLabel(RU, "some_id", "some_key", "some_value")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err = applicator.RemoveLabelTxn(ctx, RU, "some_id", "some_key")
	if err != nil {
		t.Fatal(err)
	}

	// now mutate the same label entry out of band and make sure the
	// transaction fails (which should happen because it is configured as a
	// "cas" operation)
	err = applicator.SetLabel(RU, "some_id", "some_other_key", "some_other_value")
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err == nil {
		t.Fatal("expected the transaction to fail when the labels are changed out from under it")
	}
}

func TestSetLabelsTxn(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := NewConsulApplicator(fixture.Client, 0)

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err := applicator.SetLabelsTxn(ctx, RU, "some_id", map[string]string{"some_key": "some_value"})
	if err != nil {
		t.Fatal(err)
	}

	// confirm that the label was not set yet because the transaction wasn't committed
	labeled, err := applicator.GetLabels(RU, "some_id")
	if err != nil {
		t.Fatal(err)
	}

	if len(labeled.Labels) != 0 {
		t.Fatal("labels were set before transaction was committed!")
	}

	err = transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	// confirm that the label was now set
	labeled, err = applicator.GetLabels(RU, "some_id")
	if err != nil {
		t.Fatal(err)
	}

	if labeled.Labels["some_key"] != "some_value" {
		t.Errorf("expected value for %q to be %q but was %q", "some_key", "some_value", labeled.Labels["some_key"])
	}
}

func TestSetLabelsTxnFailsIfLabelsChange(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := NewConsulApplicator(fixture.Client, 0)

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err := applicator.SetLabelsTxn(ctx, RU, "some_id", map[string]string{"some_key": "some_value"})
	if err != nil {
		t.Fatal(err)
	}

	// now mutate the same label entry out of band and make sure the
	// transaction fails (which should happen because it is configured as a
	// "cas" operation)
	err = applicator.SetLabel(RU, "some_id", "some_other_key", "some_other_value")
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err == nil {
		t.Fatal("expected the transaction to fail when the labels are changed out from under it")
	}
}

func TestRemoveLabelsTxn(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := NewConsulApplicator(fixture.Client, 0)

	// set some labels
	err := applicator.SetLabel(RU, "some_id", "some_key", "some_value")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err = applicator.RemoveLabelsTxn(ctx, RU, "some_id", []string{"some_key"})
	if err != nil {
		t.Fatal(err)
	}

	// confirm that the label was not removed yet because the transaction wasn't committed
	labeled, err := applicator.GetLabels(RU, "some_id")
	if err != nil {
		t.Fatal(err)
	}

	if labeled.Labels["some_key"] != "some_value" {
		t.Errorf("expected value for %q to be %q but was %q", "some_key", "some_value", labeled.Labels["some_key"])
	}

	err = transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	// confirm that the label was now set
	labeled, err = applicator.GetLabels(RU, "some_id")
	if err != nil {
		t.Fatal(err)
	}

	if len(labeled.Labels) != 0 {
		t.Errorf("labels were not deleted as expected, found %s", labeled.Labels)
	}
}

func TestRemoveLabelsTxnFailsIfLabelsChange(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	applicator := NewConsulApplicator(fixture.Client, 0)

	// set some labels
	err := applicator.SetLabel(RU, "some_id", "some_key", "some_value")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	err = applicator.RemoveLabelsTxn(ctx, RU, "some_id", []string{"some_key"})
	if err != nil {
		t.Fatal(err)
	}

	// now mutate the same label entry out of band and make sure the
	// transaction fails (which should happen because it is configured as a
	// "cas" operation)
	err = applicator.SetLabel(RU, "some_id", "some_other_key", "some_other_value")
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.Commit(ctx, cancelFunc, fixture.Client.KV())
	if err == nil {
		t.Fatal("expected the transaction to fail when the labels are changed out from under it")
	}
}
