package consul

import (
	"fmt"
	"testing"
)

const (
	testHostname = "some_host.com"
	testPodId    = "some_pod_id"
)

func TestNodePathHappy(t *testing.T) {
	nodePath, err := nodePath(INTENT_TREE, testHostname)
	if err != nil {
		t.Errorf("should not have errored retrieving node path: %s", err)
	}

	expected := fmt.Sprintf("%s/%s", INTENT_TREE, testHostname)
	if nodePath != expected {
		t.Errorf(
			"Unexpected value for nodePath, wanted '%s' got '%s'",
			expected,
			nodePath,
		)
	}
}

func TestNodePathErrorsWhenNoNodeName(t *testing.T) {
	_, err := nodePath(INTENT_TREE, "")
	if err == nil {
		t.Errorf("Should have gotten an error passing empty hostname")
	}
}

func TestHookPathIsExceptionToNodeNameRule(t *testing.T) {
	_, err := nodePath(HOOK_TREE, "")
	if err != nil {
		t.Errorf("Should not have errored omitting hostname when creating hook path: %s", err)
	}
}

func TestHookPathNodeNameIsIgnored(t *testing.T) {
	nodePath, err := nodePath(HOOK_TREE, testHostname)
	if err != nil {
		t.Errorf("Should not have errored retrieving node path: %s", err)
	}

	expected := fmt.Sprintf("%s", HOOK_TREE)
	if nodePath != expected {
		t.Errorf("Unexpected value for nodePath, wanted '%s' got '%s'",
			expected,
			nodePath,
		)
	}
}

func TestPodPath(t *testing.T) {
	podPath, err := PodPath(REALITY_TREE, testHostname, testPodId)
	if err != nil {
		t.Errorf("should not have errored retrieving pod path: %s", err)
	}

	expected := fmt.Sprintf("%s/%s/%s", REALITY_TREE, testHostname, testPodId)
	if podPath != expected {
		t.Errorf("Unexpected value for podPath, wanted '%s' got '%s'",
			expected,
			podPath,
		)
	}
}

func TestPodPathErrorNoHostname(t *testing.T) {
	_, err := PodPath(REALITY_TREE, "", testPodId)
	if err == nil {
		t.Errorf("should have errored retrieving pod path with empty nodeName")
	}
}
