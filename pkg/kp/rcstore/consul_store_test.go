package rcstore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/rc/fields"

	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
)

func TestPublishLatestRCs(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})
	defer close(quitCh)

	outCh, errCh := publishLatestRCs(inCh, quitCh)
	go func() {
		select {
		case <-quitCh:
		case err := <-errCh:
			t.Fatalf("Unexpected error on errCh: %s", err)
		}
	}()

	var val []fields.RC
	// Put some values on the inCh and read them from outCh transformed
	// into RCs
	inCh <- rcsWithIDs(t, "a", 3)
	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 3 {
		t.Errorf("Expected %d values on outCh, got %d", 3, len(val))
	}

	for _, rc := range val {
		if rc.ID.String() != "a" {
			t.Errorf("Expected all RCs to have id %s, was %s", "a", rc.ID)
		}
	}

	inCh <- rcsWithIDs(t, "b", 2)
	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 2 {
		t.Errorf("Expected %d values on outCh, got %d", 2, len(val))
	}

	for _, rc := range val {
		if rc.ID.String() != "b" {
			t.Errorf("Expected all RCs to have id %s, was %s", "b", rc.ID)
		}
	}

	// Now, let's put some stuff on inCh but not read it for a bit
	inCh <- rcsWithIDs(t, "c", 4)
	inCh <- rcsWithIDs(t, "d", 5)
	inCh <- rcsWithIDs(t, "e", 6)

	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 6 {
		t.Errorf("Expected %d values on outCh, got %d", 6, len(val))
	}

	for _, rc := range val {
		if rc.ID.String() != "e" {
			t.Errorf("Expected all RCs to have id %s, was %s", "e", rc.ID)
		}
	}
}

func TestPublishLatestRCsSkipsIfCorrupt(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})
	defer close(quitCh)

	outCh, errCh := publishLatestRCs(inCh, quitCh)

	// push some legitimate RCs and read them out
	var val []fields.RC
	inCh <- rcsWithIDs(t, "a", 3)
	select {
	case val = <-outCh:
	case err := <-errCh:
		t.Fatalf("Unexpected error on errCh: %s", err)
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 3 {
		t.Errorf("Expected %d values on outCh, got %d", 3, len(val))
	}

	for _, rc := range val {
		if rc.ID.String() != "a" {
			t.Errorf("Expected all RCs to have id %s, was %s", "a", rc.ID)
		}
	}

	// Now push some bogus JSON that will trigger an error
	corruptData := []*api.KVPair{&api.KVPair{Value: []byte("bad_json")}}
	inCh <- corruptData

	select {
	case val = <-outCh:
		t.Fatalf("Didn't expect out value for bogus input")
	case <-errCh:
		// good
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out reading from channel")
	}

	// Now push more legitimate stuff and make sure that is written
	// push some legitimate RCs and read them out
	inCh <- rcsWithIDs(t, "b", 3)
	select {
	case val = <-outCh:
	case err := <-errCh:
		t.Fatalf("Unexpected error on errCh: %s", err)
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 3 {
		t.Errorf("Expected %d values on outCh, got %d", 3, len(val))
	}

	for _, rc := range val {
		if rc.ID.String() != "b" {
			t.Errorf("Expected all RCs to have id %s, was %s", "b", rc.ID)
		}
	}
}

func TestPublishQuitsOnQuitChannelClose(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})

	outCh, errCh := publishLatestRCs(inCh, quitCh)
	close(quitCh)

	select {
	case _, ok := <-outCh:
		if ok {
			t.Fatalf("outCh should have closed since quitCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for outCh to close")
	}

	select {
	case _, ok := <-errCh:
		if ok {
			t.Fatalf("errCh should have closed since quitCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for errCh to close")
	}
}

func TestPublishQuitsOnInChannelCloseBeforeData(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})
	defer close(quitCh)

	outCh, errCh := publishLatestRCs(inCh, quitCh)
	close(inCh)

	select {
	case _, ok := <-outCh:
		if ok {
			t.Fatalf("outCh should have closed since inCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for outCh to close")
	}

	select {
	case _, ok := <-errCh:
		if ok {
			t.Fatalf("errCh should have closed since inCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for errCh to close")
	}
}

func TestPublishQuitsOnInChannelCloseAfterData(t *testing.T) {
	inCh := make(chan api.KVPairs)
	quitCh := make(chan struct{})
	defer close(quitCh)

	outCh, errCh := publishLatestRCs(inCh, quitCh)

	// Write some legitimate data and read it out
	var val []fields.RC
	// Put some values on the inCh and read them from outCh transformed
	// into RCs
	inCh <- rcsWithIDs(t, "a", 3)
	select {
	case val = <-outCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out reading from channel")
	}

	if len(val) != 3 {
		t.Errorf("Expected %d values on outCh, got %d", 3, len(val))
	}

	for _, rc := range val {
		if rc.ID.String() != "a" {
			t.Errorf("Expected all RCs to have id %s, was %s", "a", rc.ID)
		}
	}

	close(inCh)

	select {
	case _, ok := <-outCh:
		if ok {
			t.Fatalf("outCh should have closed since inCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for outCh to close")
	}

	select {
	case _, ok := <-errCh:
		if ok {
			t.Fatalf("errCh should have closed since inCh closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for errCh to close")
	}
}

func rcsWithIDs(t *testing.T, id string, num int) api.KVPairs {
	var pairs api.KVPairs
	builder := pods.NewManifestBuilder()
	builder.SetID("slug")
	manifest := builder.GetManifest()
	for i := 0; i < num; i++ {
		rc := fields.RC{
			ID:       fields.ID(id),
			Manifest: manifest,
		}

		jsonRC, err := json.Marshal(rc)
		if err != nil {
			t.Fatalf("Unable to marshal test RC as json: %s", err)
		}

		pairs = append(pairs, &api.KVPair{
			Value: jsonRC,
		})
	}

	return pairs
}
