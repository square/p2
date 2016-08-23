package kp

import (
	"fmt"
	"testing"

	"github.com/pborman/uuid"
)

func TestGetHealthNoEntry(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	// Get a get without a key
	_, err := f.Store.GetHealth("testservice", "testnode")
	if err != nil {
		t.Fatalf("GetHealth returned an error: %v", err)
	}
	_, err = f.Store.GetServiceHealth("testservice")
	if err != nil {
		t.Fatalf("GetServiceHealth returned an error: %v", err)
	}
}

func TestGetHealthWithEntry(t *testing.T) {
	f := NewConsulTestFixture(t)
	defer f.Close()

	// Put the key
	watch := WatchResult{
		Id:      "id",
		Node:    "node",
		Service: "service",
	}
	_, _, err := f.Store.PutHealth(watch)
	if err != nil {
		t.Fatalf("PutHealth failed: %v", err)
	}

	// Get should work
	watchRes, err := f.Store.GetHealth(watch.Service, watch.Node)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if watchRes.Id != watch.Id {
		t.Fatalf("watchRes and watch ids did not match. GetHealth failed: %#v", watchRes)
	}
	if watchRes.Node != watch.Node {
		t.Fatalf("watchRes and watch Node did not match. GetHealth failed: %#v", watchRes)
	}
	if watchRes.Service != watch.Service {
		t.Fatalf("watchRes and watch Service did not match. GetHealth failed: %#v", watchRes)
	}
}

func TestPodUniqueKeyFromConsulPath(t *testing.T) {
	type expectation struct {
		path string
		err  bool
		uuid bool
		str  string
	}

	uuid := uuid.New()
	expectations := []expectation{
		{
			path: "intent/example.com/mysql",
			err:  false,
			uuid: false,
		},
		{
			path: "reality/example.com/mysql",
			err:  false,
			uuid: false,
		},
		{
			path: "labels/example.com/mysql",
			err:  true,
		},
		{
			path: "intent/example/com/mysql",
			err:  true,
		},
		{
			path: "hooks/all_hooks",
			err:  false,
			uuid: false,
		},
		{
			path: fmt.Sprintf("intent/example.com/%s", uuid),
			uuid: true,
			str:  uuid,
			err:  false,
		},
	}

	for _, expectation := range expectations {
		podUniqueKey, err := PodUniqueKeyFromConsulPath(expectation.path)
		if expectation.err {
			if err == nil {
				t.Errorf("Expected an error for key '%s'", expectation.path)
			}
			continue
		}

		if err != nil {
			t.Error("Unexpected error for key '%s': %s", expectation.path, err)
			continue
		}

		if (podUniqueKey != nil) != expectation.uuid {
			t.Errorf("Expected (podUniqueKey != nil) to be %t, was %t", expectation.uuid, podUniqueKey != nil)
		}

		if expectation.uuid && podUniqueKey.ID != expectation.str {
			t.Errorf("Expected key string to be %s, was %t", expectation.str, podUniqueKey.ID)
		}
	}
}
