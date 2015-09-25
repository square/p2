package inspect

import (
	"fmt"
	"sort"
	"strings"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
)

const (
	INTENT_SOURCE = iota
	REALITY_SOURCE
)

type NodePodStatus struct {
	NodeName           string             `json:"node,omitempty"`
	PodId              string             `json:"pod,omitempty"`
	IntentManifestSHA  string             `json:"intent_manifest_sha"`
	RealityManifestSHA string             `json:"reality_manifest_sha"`
	IntentLocations    []string           `json:"intent_locations"`
	RealityLocations   []string           `json:"reality_locations"`
	Health             health.HealthState `json:"health,omitempty"`
}

func AddKVPToMap(result kp.ManifestResult, source int, filterNode, filterPod string, statuses map[string]map[string]NodePodStatus) error {
	keySegs := strings.Split(result.Path, "/")
	nodeName := keySegs[1]
	podId := keySegs[2]

	if filterNode != "" && nodeName != filterNode {
		return nil
	}
	if filterPod != "" && podId != filterPod {
		return nil
	}

	if statuses[podId] == nil {
		statuses[podId] = make(map[string]NodePodStatus)
	}
	old := statuses[podId][nodeName]

	manifestSHA, err := result.Manifest.SHA()
	if err != nil {
		return err
	}

	switch source {
	case INTENT_SOURCE:
		if old.IntentManifestSHA != "" {
			return fmt.Errorf("Two intent manifests for node %s pod %s", nodeName, podId)
		}
		old.IntentManifestSHA = manifestSHA
		for _, launchable := range result.Manifest.GetLaunchableStanzas() {
			old.IntentLocations = append(old.IntentLocations, launchable.Location)
		}
		sort.Strings(old.IntentLocations)
	case REALITY_SOURCE:
		if old.RealityManifestSHA != "" {
			return fmt.Errorf("Two reality manifests for node %s pod %s", nodeName, podId)
		}
		old.RealityManifestSHA = manifestSHA
		for _, launchable := range result.Manifest.GetLaunchableStanzas() {
			old.RealityLocations = append(old.RealityLocations, launchable.Location)
		}
		sort.Strings(old.RealityLocations)
	}

	statuses[podId][nodeName] = old
	return nil
}
