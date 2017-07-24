package inspect

import (
	"fmt"
	"sort"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/types"
)

const (
	INTENT_SOURCE = iota
	REALITY_SOURCE
)

type LaunchableVersion struct {
	Location string                    `json:"location,omitempty"`
	Version  *launch.LaunchableVersion `json:"version,omitempty"`
}

type NodePodStatus struct {
	NodeName           types.NodeName                            `json:"node,omitempty"`
	PodId              types.PodID                               `json:"pod,omitempty"`
	IntentManifestSHA  string                                    `json:"intent_manifest_sha"`
	RealityManifestSHA string                                    `json:"reality_manifest_sha"`
	IntentVersions     map[launch.LaunchableID]LaunchableVersion `json:"intent_versions,omitempty"`
	RealityVersions    map[launch.LaunchableID]LaunchableVersion `json:"reality_versions,omitempty"`
	Health             health.HealthState                        `json:"health,omitempty"`

	// These fields are kept for backwards compatibility with tools that
	// parse the output of p2-inspect. intent_versions and reality_versions
	// are preferred since those handle multiple versions of manifest syntax
	IntentLocations  []string `json:"intent_locations"`
	RealityLocations []string `json:"reality_locations"`
}

func AddKVPToMap(result consul.ManifestResult, source int, filterNode types.NodeName, filterPod types.PodID, statuses map[types.PodID]map[types.NodeName]NodePodStatus) error {
	if result.PodUniqueKey != "" {
		// for now, p2-inspect won't show uuid pods
		return nil
	}
	nodeName := result.PodLocation.Node
	podId := result.Manifest.ID()

	if filterNode != "" && nodeName != filterNode {
		return nil
	}
	if filterPod != "" && podId != filterPod {
		return nil
	}

	if statuses[podId] == nil {
		statuses[podId] = make(map[types.NodeName]NodePodStatus)
	}
	old := statuses[podId][nodeName]
	old.IntentVersions = make(map[launch.LaunchableID]LaunchableVersion)
	old.RealityVersions = make(map[launch.LaunchableID]LaunchableVersion)

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
		for launchableID, launchable := range result.Manifest.GetLaunchableStanzas() {
			var version *launch.LaunchableVersion
			if launchable.Version.ID != "" {
				version = &launchable.Version
			}

			old.IntentVersions[launchableID] = LaunchableVersion{
				Location: launchable.Location,
				Version:  version,
			}
			old.IntentLocations = append(old.IntentLocations, launchable.Location)
		}
		sort.Strings(old.IntentLocations)
	case REALITY_SOURCE:
		if old.RealityManifestSHA != "" {
			return fmt.Errorf("Two reality manifests for node %s pod %s", nodeName, podId)
		}
		old.RealityManifestSHA = manifestSHA
		for launchableID, launchable := range result.Manifest.GetLaunchableStanzas() {
			var version *launch.LaunchableVersion

			if launchable.Version.ID != "" {
				version = &launchable.Version
			}
			old.RealityVersions[launchableID] = LaunchableVersion{
				Location: launchable.Location,
				Version:  version,
			}
			old.RealityLocations = append(old.RealityLocations, launchable.Location)
		}
		sort.Strings(old.RealityLocations)
	}

	statuses[podId][nodeName] = old
	return nil
}
