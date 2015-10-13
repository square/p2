package labels

import (
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
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

	labeled, err := app.GetMatches(selector, NODE)
	Assert(t).IsNil(err, "expected no error getting matches")

	Assert(t).AreEqual(len(labeled), 1, "expected one match")

	Assert(t).AreEqual(labeled[0].ID, "hi", "expected the ID with the right label to match")
}
