// +build !race

package labels

import (
	"context"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"

	. "github.com/anthonybishopric/gotcha"
	klabels "k8s.io/kubernetes/pkg/labels"
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

func TestMutateAndSelectHTTPApplicator(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	labelServer := NewHTTPLabelServer(NewConsulApplicator(fixture.Client, 0), 0, logging.TestLogger())
	server := httptest.NewServer(labelServer.Handler())
	defer server.Close()
	url, err := url.Parse(server.URL)
	Assert(t).IsNil(err, "expected no error parsing url")

	applicator, err := NewHTTPApplicator(nil, url)
	Assert(t).IsNil(err, "expected no error creating HTTP applicator")

	podID := "abc/123"
	colorLabel := "p2/color"

	// set a single label and assert its presence
	Assert(t).IsNil(applicator.SetLabel(POD, podID, colorLabel, "red"), "Should not have erred setting label")
	podLabels, err := applicator.GetLabels(POD, podID)
	Assert(t).IsNil(err, "Should not have erred getting labels for the pod")
	Assert(t).AreEqual("red", podLabels.Labels.Get(colorLabel), "Should have seen red on the color label")
	matches, err := applicator.GetMatches(klabels.Everything().Add(colorLabel, klabels.EqualsOperator, []string{"red"}), POD)
	Assert(t).IsNil(err, "There should not have been an error running a selector")
	Assert(t).AreEqual(1, len(matches), "Should have gotten a match")
	Assert(t).AreEqual(podID, matches[0].ID, "Wrong pod returned")
	Assert(t).AreEqual("red", matches[0].Labels.Get(colorLabel), "Wrong color returned")

	// set all labels, expect all to change
	Assert(t).IsNil(applicator.SetLabels(POD, podID, klabels.Set{colorLabel: "green", "state": "experimental"}), "Should not err setting labels")
	podLabels, err = applicator.GetLabels(POD, podID)
	Assert(t).IsNil(err, "Should not have erred getting labels for the pod")
	Assert(t).AreEqual("green", podLabels.Labels.Get(colorLabel), "Should have seen green on the color label")
	Assert(t).AreEqual("experimental", podLabels.Labels.Get("state"), "Should have seen experimental on the state label")

	// set a single label, expect only one of several labels to change
	Assert(t).IsNil(applicator.SetLabel(POD, podID, colorLabel, "orange"), "Should not have erred setting label")
	podLabels, err = applicator.GetLabels(POD, podID)
	Assert(t).IsNil(err, "Should not have erred getting labels for the pod")
	Assert(t).AreEqual("orange", podLabels.Labels.Get(colorLabel), "Should have seen orange on the color label")
	Assert(t).AreEqual("experimental", podLabels.Labels.Get("state"), "Should have seen experimental on the state label")

	// set a label on a new pod, expect list to contain two pods
	Assert(t).IsNil(applicator.SetLabel(POD, "def-456", colorLabel, "blue"), "Should not have erred setting label")
	allPodLabels, err := applicator.ListLabels(POD)
	Assert(t).AreEqual(len(allPodLabels), 2, "All labeld pods should have been returned")
	bluePod := allPodLabels[0]
	if allPodLabels[0].ID == podID {
		bluePod = allPodLabels[1]
	}
	Assert(t).AreEqual("blue", bluePod.Labels.Get(colorLabel), "Should have returned label data")

	// remove a specific label, expect only one remains
	Assert(t).IsNil(applicator.RemoveLabel(POD, podID, colorLabel), "Should not have erred removing label")
	podLabels, err = applicator.GetLabels(POD, podID)
	Assert(t).IsNil(err, "Should not have erred getting labels for the pod")
	Assert(t).AreEqual("experimental", podLabels.Labels.Get("state"), "Should have seen experimental on the state label")
	Assert(t).AreEqual(1, len(podLabels.Labels), "Should have only had one label")

	// remove all labels, expect none left
	Assert(t).IsNil(applicator.RemoveAllLabels(POD, podID), "Should not have erred removing labels")
	podLabels, err = applicator.GetLabels(POD, podID)
	Assert(t).IsNil(err, "Should not have erred getting labels for the pod")
	Assert(t).AreEqual(0, len(podLabels.Labels), "Should have only had one label")
}
