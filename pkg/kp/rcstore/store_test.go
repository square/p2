package rcstore

import (
	"fmt"
	"testing"

	"github.com/square/p2/pkg/kp"
)

const testRCId = "abcd-1234"

func TestRCPathHappy(t *testing.T) {
	rcPath, err := rcPath(testRCId)
	if err != nil {
		t.Fatalf("Unable to compute rc path: %s", err)
	}

	expected := fmt.Sprintf("%s/%s", rcTree, testRCId)
	if rcPath != expected {
		t.Errorf("Unexpected value for rcPath, wanted '%s' got '%s'",
			expected,
			rcPath,
		)
	}
}

func TestRCPathErrorNoID(t *testing.T) {
	_, err := rcPath("")
	if err == nil {
		t.Errorf("Should have errored retrieving rc path with empty rcID")
	}
}

func TestRCLockPath(t *testing.T) {
	rcLockPath, err := RCLockPath(testRCId)
	if err != nil {
		t.Fatalf("Unable to compute rc lock path: %s", err)
	}

	expected := fmt.Sprintf("%s/%s/%s", kp.LOCK_TREE, rcTree, testRCId)
	if rcLockPath != expected {
		t.Errorf("Unexpected value for rcLockPath, wanted '%s' got '%s'",
			expected,
			rcLockPath,
		)
	}
}

func TestRCLockPathErrorNoID(t *testing.T) {
	_, err := RCLockPath("")
	if err == nil {
		t.Errorf("Expected error computing rc lock path with no id")
	}
}

func TestRCUpdateLockPath(t *testing.T) {
	rcUpdateLockPath, err := RCUpdateLockPath(testRCId)
	if err != nil {
		t.Fatalf("Unable to compute rc update lock path: %s", err)
	}

	expected := fmt.Sprintf("%s/%s/%s/update", kp.LOCK_TREE, rcTree, testRCId)
	if rcUpdateLockPath != expected {
		t.Errorf("Unexpected value for rcUpdateLockPath, wanted '%s' got '%s'",
			expected,
			rcUpdateLockPath,
		)
	}
}

func TestRCUpdateLockPathErrorNoID(t *testing.T) {
	_, err := RCUpdateLockPath("")
	if err == nil {
		t.Errorf("Expected error computing rc update lock path with no id")
	}
}
