// +build !race

package labels

import (
	"testing"
	"time"

	"github.com/square/p2/pkg/store/consul/consulutil"
)

// listerWrapper just wraps a labelLister but also records the calls it gets so
// tests can inspect them
type listerWrapper struct {
	inner labelLister

	calls []Type
}

func (l *listerWrapper) ListLabels(labelType Type) ([]Labeled, error) {
	l.calls = append(l.calls, labelType)
	return l.inner.ListLabels(labelType)
}

func TestRetrieveStaleHappy(t *testing.T) {
	f := consulutil.NewFixture(t)

	applicator := NewConsulApplicator(f.Client, 0)
	batcher := NewBatcher(&listerWrapper{
		inner: applicator,
	}, 1*time.Millisecond)

	// write some data
	err := applicator.SetLabels(NODE, "node1", map[string]string{"hostname": "node1"})
	if err != nil {
		t.Fatal(err)
	}
	err = applicator.SetLabels(NODE, "node2", map[string]string{"hostname": "node2"})
	if err != nil {
		t.Fatal(err)
	}

	allLabeled, err := batcher.ForType(NODE).RetrieveStale()
	if err != nil {
		t.Fatal(err)
	}

	if len(allLabeled) != 2 {
		t.Errorf("expected 2 label results but got %d", len(allLabeled))
	}
}

func TestRetrieveStaleDoesntBlock(t *testing.T) {
	f := consulutil.NewFixture(t)

	applicator := NewConsulApplicator(f.Client, 0)
	batcher := NewBatcher(&listerWrapper{
		inner: applicator,
	}, 1*time.Minute) // set the batch time very high

	// write some data
	err := applicator.SetLabels(NODE, "node1", map[string]string{"hostname": "node1"})
	if err != nil {
		t.Fatal(err)
	}
	err = applicator.SetLabels(NODE, "node2", map[string]string{"hostname": "node2"})
	if err != nil {
		t.Fatal(err)
	}

	// initialize the staleCache
	batcher.ForType(NODE).staleCacheMux.Lock()
	batcher.ForType(NODE).staleCache = batchResult{
		Matches: []Labeled{
			{
				ID: "whatever",
			},
		},
	}
	batcher.ForType(NODE).staleCacheMux.Unlock()

	allLabeled, err := batcher.ForType(NODE).RetrieveStale()
	if err != nil {
		t.Fatal(err)
	}

	if len(allLabeled) != 1 {
		t.Errorf("expected 1 label result but got %d", len(allLabeled))
	}
}

func TestRetrieveStaleByID(t *testing.T) {
	f := consulutil.NewFixture(t)

	applicator := NewConsulApplicator(f.Client, 0)
	batcher := NewBatcher(&listerWrapper{
		inner: applicator,
	}, 1*time.Millisecond)

	// write some data
	err := applicator.SetLabels(NODE, "node1", map[string]string{"hostname": "node1"})
	if err != nil {
		t.Fatal(err)
	}
	err = applicator.SetLabels(NODE, "node2", map[string]string{"hostname": "node2"})
	if err != nil {
		t.Fatal(err)
	}

	labeled, err := batcher.ForType(NODE).RetrieveStaleByID("node1")
	if err != nil {
		t.Fatal(err)
	}

	if labeled.ID != "node1" {
		t.Errorf("expected label ID to be %s but was %s", "node1", labeled.ID)
	}

	if labeled.Labels["hostname"] != "node1" {
		t.Errorf("expected hostname label value for %s to be %s but was %s", "node1", "node1", labeled.Labels["hostname"])
	}
}

func TestRetrieveStaleByIDMissingID(t *testing.T) {
	f := consulutil.NewFixture(t)

	applicator := NewConsulApplicator(f.Client, 0)
	batcher := NewBatcher(&listerWrapper{
		inner: applicator,
	}, 1*time.Millisecond)

	// write some data
	err := applicator.SetLabels(NODE, "node1", map[string]string{"hostname": "node1"})
	if err != nil {
		t.Fatal(err)
	}
	err = applicator.SetLabels(NODE, "node2", map[string]string{"hostname": "node2"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = batcher.ForType(NODE).RetrieveStaleByID("node3")
	if err == nil {
		t.Fatal("expected an error retrieving labels for an ID that doesn't exist")
	}
}
