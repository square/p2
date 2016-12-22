package store

import (
	"encoding/json"
	"testing"
)

func TestZeroUnmarshal(t *testing.T) {
	var ds DaemonSet
	err := json.Unmarshal([]byte(`{}`), &ds)
	if err != nil {
		t.Fatal("error unmarshaling:", err)
	}
}
