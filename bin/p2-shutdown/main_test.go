package main

import (
	"testing"

	"github.com/square/p2/pkg/types"
)

func TestShutdownPod(t *testing.T) {
	testCases := []struct {
		pod            types.PodID
		podsToShutdown []types.PodID
		podsToExclude  []types.PodID
		expectation    bool
	}{
		{types.PodID("a"), []types.PodID{"a"}, []types.PodID{}, true},
		{types.PodID("a"), []types.PodID{}, []types.PodID{"a"}, false},
		{types.PodID("a"), []types.PodID{}, []types.PodID{}, true},
		{types.PodID("a"), []types.PodID{"b"}, []types.PodID{}, false},
	}

	for i, testCase := range testCases {
		shouldShutdown := shouldShutdownPod(testCase.pod, testCase.podsToShutdown, testCase.podsToExclude)
		if shouldShutdown != testCase.expectation {
			t.Errorf("Test Case: %d, Expected: %v, Got: %v", i, testCase.expectation, shouldShutdown)
		}
	}

}
