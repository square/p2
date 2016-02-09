package rollstore

import (
	"fmt"
	"testing"

	"github.com/square/p2/pkg/kp"
)

const testRCId = "abcd-1234"

func TestRollPath(t *testing.T) {
	rollPath, err := RollPath(testRCId)
	if err != nil {
		t.Fatalf("Unable to compute roll path: %s", err)
	}

	expected := fmt.Sprintf("%s/%s", rollTree, testRCId)
	if rollPath != expected {
		t.Errorf("Unexpected value for rollPath, wanted '%s' got '%s'",
			expected,
			rollPath,
		)
	}
}

func TestRollPathErrorNoID(t *testing.T) {
	_, err := RollPath("")
	if err == nil {
		t.Errorf("Expected error computing roll path with no id")
	}
}

func TestRollLockPath(t *testing.T) {
	rollLockPath, err := RollLockPath(testRCId)
	if err != nil {
		t.Fatalf("Unable to compute roll lock path: %s", err)
	}

	expected := fmt.Sprintf("%s/%s/%s", kp.LOCK_TREE, rollTree, testRCId)
	if rollLockPath != expected {
		t.Errorf("Unexpected value for rollLockPath, wanted '%s' got '%s'",
			expected,
			rollLockPath,
		)
	}
}

func TestRollLockPathErrorNoID(t *testing.T) {
	_, err := RollLockPath("")
	if err == nil {
		t.Errorf("Expected error computing roll lock path with no id")
	}
}
