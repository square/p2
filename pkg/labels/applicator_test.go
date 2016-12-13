package labels

import (
	"testing"

	"k8s.io/kubernetes/pkg/labels"
)

func TestNothingReallyMatchesNothing(t *testing.T) {
	check := func(sel labels.Selector) {
		if sel.Matches(labels.Set{}) {
			t.Fatalf("Should not have matched the empty set: %v", sel.String())
		}
		if sel.Matches(labels.Set{"x": "1"}) {
			t.Fatalf("Should not have matched x=1: %v", sel.String())
		}
		if sel.Matches(labels.Set{"x": "0"}) {
			t.Fatalf("Should not have matched x=0: %v", sel.String())
		}
		if sel.Matches(labels.Set{"x": "2"}) {
			t.Fatalf("Should not have matched x=2: %v", sel.String())
		}
	}
	check(Nothing())
	// test that it can be serialized
	sel, err := labels.Parse(Nothing().String())
	if err != nil {
		t.Fatal(err)
	}
	check(sel)
}
