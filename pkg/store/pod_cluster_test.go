package store

import (
	"k8s.io/kubernetes/pkg/labels"

	"testing"
)

func TestPodEqualityWhenBothNil(t *testing.T) {
	var x *PodCluster = nil
	var y *PodCluster = nil
	if !x.Equals(y) {
		t.Fatal("Nil clusters should have been equal")
	}
}

func equalPodClusters() (*PodCluster, *PodCluster) {
	a := &PodCluster{
		ID:               "abc123",
		PodID:            "carrot",
		Name:             "production",
		AvailabilityZone: "east",
		Annotations: map[string]interface{}{
			"key1": "value",
			"key2": 2,
		},
		PodSelector: labels.Everything().Add("environment", labels.EqualsOperator, []string{"fancy"}),
	}
	b := &PodCluster{
		ID:               "abc123",
		PodID:            "carrot",
		Name:             "production",
		AvailabilityZone: "east",
		Annotations: map[string]interface{}{
			"key1": "value",
			"key2": 2,
		},
		PodSelector: labels.Everything().Add("environment", labels.EqualsOperator, []string{"fancy"}),
	}
	return a, b
}

func TestPodEquality(t *testing.T) {
	a, b := equalPodClusters()
	if !a.Equals(b) || !b.Equals(a) {
		t.Fatal("precondition failed: a did not equal b")
	}
	assertNotEqual := func(fieldName string) {
		if a.Equals(b) {
			t.Fatalf("a.Equals(b) should not have been true: field tested: %v", fieldName)
		}
		if b.Equals(a) {
			t.Fatalf("b.Equals(a) should not have been true: field tested: %v", fieldName)
		}
	}
	a.ID = "def456"
	assertNotEqual("ID")
	a.ID = b.ID

	a.PodID = "def456"
	assertNotEqual("PodID")
	a.PodID = b.PodID

	a.Name = "def456"
	assertNotEqual("Name")
	a.Name = b.Name

	a.AvailabilityZone = "def456"
	assertNotEqual("AvailabilityZone")
	a.AvailabilityZone = b.AvailabilityZone

	a.Annotations = map[string]interface{}{
		"something_else": "hey",
	}
	assertNotEqual("Annotations")
	a.Annotations = b.Annotations

	a.PodSelector = nil
	assertNotEqual("nil pod selector")
	a.PodSelector = labels.Everything().Add("environment", labels.EqualsOperator, []string{"special"})
	assertNotEqual("different pod selector")
	a.PodSelector = b.PodSelector

	ax := a
	a = nil
	assertNotEqual("nil a")
	a = ax

	if !a.Equals(b) || !b.Equals(a) {
		t.Fatal("postcondition failed: a did not equal b")
	}
}
