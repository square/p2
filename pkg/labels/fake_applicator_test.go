package labels

import (
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"k8s.io/kubernetes/pkg/labels"
)

// The Fake Applicator is most useful in tests
// But for us to be sure it's useful, we also have to test it.

func TestEmptyGet(t *testing.T) {
	app := NewFakeApplicator()

	labeled, err := app.GetLabels(NODE, "hi")
	Assert(t).IsNil(err, "expected no error getting labels")

	Assert(t).AreEqual(len(labeled.Labels), 0, "expected no labels")
}

func TestSetThenGet(t *testing.T) {
	app := NewFakeApplicator()

	err := app.SetLabel(NODE, "hi", "foo", "bar")
	Assert(t).IsNil(err, "expected no error setting label")

	labeled, err := app.GetLabels(NODE, "hi")
	Assert(t).IsNil(err, "expected no error getting labels")

	Assert(t).AreEqual(len(labeled.Labels), 1, "expected one label")
	Assert(t).AreEqual(labeled.Labels["foo"], "bar", "expected label value to match what was set earlier")
}

func TestRemove(t *testing.T) {
	app := NewFakeApplicator()

	err := app.SetLabel(NODE, "hi", "foo", "bar")
	Assert(t).IsNil(err, "expected no error setting label")

	err = app.RemoveLabel(NODE, "hi", "foo")
	Assert(t).IsNil(err, "expected no error removing label")

	labeled, err := app.GetLabels(NODE, "hi")
	Assert(t).IsNil(err, "expected no error getting labels")

	Assert(t).AreEqual(len(labeled.Labels), 0, "expected no labels")
}

func TestMatches(t *testing.T) {
	app := NewFakeApplicator()

	err := app.SetLabel(NODE, "hi", "foo", "bar")
	Assert(t).IsNil(err, "expected no error setting label")
	err = app.SetLabel(NODE, "bye", "you", "bar")
	Assert(t).IsNil(err, "expected no error setting label")
	err = app.SetLabel(NODE, "stuff", "foo", "blar")
	Assert(t).IsNil(err, "expected no error setting label")

	selector := labels.Everything().Add("foo", labels.EqualsOperator, []string{"bar"})

	labeled, err := app.GetMatches(selector, NODE, false)
	Assert(t).IsNil(err, "expected no error getting matches")

	Assert(t).AreEqual(len(labeled), 1, "expected one match")

	Assert(t).AreEqual(labeled[0].ID, "hi", "expected the ID with the right label to match")
}

const waitTime = 100 * time.Millisecond

// This function will construct a batch of changes and return them
func getBatchedChanges(
	t *testing.T,
	inCh <-chan *LabeledChanges,
) *LabeledChanges {
	var currChanges *LabeledChanges
	numChanges := 5
	finalChanges := &LabeledChanges{}

	for i := 0; i < numChanges; i++ {
		select {
		case currChanges = <-inCh:
			for _, created := range currChanges.Created {
				finalChanges.Created = append(finalChanges.Created, created)
			}
			for _, updated := range currChanges.Updated {
				finalChanges.Updated = append(finalChanges.Updated, updated)
			}
			for _, deleted := range currChanges.Deleted {
				finalChanges.Deleted = append(finalChanges.Deleted, deleted)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Expected something on channel but found nothing")
		}
	}

	return finalChanges
}

func TestFakeWatchMatchDiff(t *testing.T) {
	app := NewFakeApplicator()

	quitCh := make(chan struct{})
	defer close(quitCh)
	inCh := app.WatchMatchDiff(labels.Everything(), NODE, quitCh)

	var changes *LabeledChanges
	changes = getBatchedChanges(t, inCh)
	Assert(t).AreEqual(len(changes.Created), 0, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 0, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 0, "expected number of deleted labels to match")

	// Create a label and verify that it was created
	err := app.SetLabel(NODE, "node1", "foo", "bar")
	Assert(t).IsNil(err, "expected no error setting label")

	changes = getBatchedChanges(t, inCh)
	Assert(t).AreEqual(len(changes.Created), 1, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 0, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 0, "expected number of deleted labels to match")

	// Create another label and update one and verify
	err = app.SetLabel(NODE, "node2", "foo", "bar")
	Assert(t).IsNil(err, "expected no error setting label")
	err = app.SetLabel(NODE, "node1", "foo", "foo")
	Assert(t).IsNil(err, "expected no error setting label")

	changes = getBatchedChanges(t, inCh)
	Assert(t).AreEqual(len(changes.Created), 1, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 1, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 0, "expected number of deleted labels to match")

	// Delete a label and create one
	err = app.RemoveAllLabels(NODE, "node1")
	Assert(t).IsNil(err, "expected no error removing labels")
	err = app.SetLabel(NODE, "node3", "foo", "bar")
	Assert(t).IsNil(err, "expected no error setting label")

	changes = getBatchedChanges(t, inCh)
	Assert(t).AreEqual(len(changes.Created), 1, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 0, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 1, "expected number of deleted labels to match")

	// Create, Update, and Delete a label
	err = app.SetLabel(NODE, "node4", "foo", "bar")
	Assert(t).IsNil(err, "expected no error setting label")
	err = app.SetLabel(NODE, "node3", "foo", "foo")
	Assert(t).IsNil(err, "expected no error setting label")
	err = app.RemoveAllLabels(NODE, "node2")
	Assert(t).IsNil(err, "expected no error removing labels")

	changes = getBatchedChanges(t, inCh)
	Assert(t).AreEqual(len(changes.Created), 1, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 1, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 1, "expected number of deleted labels to match")

	// Remove the remaining two labels
	err = app.RemoveAllLabels(NODE, "node3")
	Assert(t).IsNil(err, "expected no error removing labels")
	err = app.RemoveAllLabels(NODE, "node4")
	Assert(t).IsNil(err, "expected no error removing labels")

	changes = getBatchedChanges(t, inCh)
	Assert(t).AreEqual(len(changes.Created), 0, "expected number of created labels to match")
	Assert(t).AreEqual(len(changes.Updated), 0, "expected number of updated labels to match")
	Assert(t).AreEqual(len(changes.Deleted), 2, "expected number of deleted labels to match")
}
