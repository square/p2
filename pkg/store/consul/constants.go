package consul

import (
	"path"

	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

// PodPrefix represents the top level of a subtree whose leaves are expected to
// be pods, e.g. "hooks", "intent", "reality"
type PodPrefix string

func (p PodPrefix) String() string { return string(p) }

const (
	INTENT_TREE  PodPrefix = "intent"
	REALITY_TREE PodPrefix = "reality"
	HOOK_TREE    PodPrefix = "hooks"
	LOCK_TREE              = "lock"
)

func nodePath(podPrefix PodPrefix, nodeName types.NodeName) (string, error) {
	// hook tree is an exception to the rule because they are not scheduled
	// by host, and it is valid to want to watch for them agnostic to pod
	// id. There are plans to deploy hooks by host at which time this
	// exception can be removed
	if podPrefix == HOOK_TREE {
		nodeName = ""
	} else {
		if nodeName == "" {
			return "", util.Errorf("nodeName not specified when computing host path")
		}
	}

	return path.Join(string(podPrefix), nodeName.String()), nil
}

func podPath(podPrefix PodPrefix, nodeName types.NodeName, podId types.PodID) (string, error) {
	nodePath, err := nodePath(podPrefix, nodeName)
	if err != nil {
		return "", err
	}

	if podId == "" {
		return "", util.Errorf("pod id not specified when computing pod path")
	}

	return path.Join(nodePath, string(podId)), nil
}

// Returns the consul path to use when intending to lock a pod, e.g.
// lock/intent/some_host/some_pod
func PodLockPath(podPrefix PodPrefix, nodeName types.NodeName, podId types.PodID) (string, error) {
	subPodPath, err := podPath(podPrefix, nodeName, podId)
	if err != nil {
		return "", err
	}

	return path.Join(LOCK_TREE, subPodPath), nil
}

// Returns the consul path to use when locking out any other pkg/replication-based deploys
// for a given pod ID
func ReplicationLockPath(podId types.PodID) string {
	return path.Join(LOCK_TREE, "replication", podId.String())
}
