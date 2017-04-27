package hooks

import (
	"fmt"
	"os"

	"github.com/square/p2/pkg/config"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

func CurrentEnv() *HookEnv {
	return &HookEnv{}
}

// Hook env is a utility for hook writers using Go. This provides
// useful access to objects exported by the hooks package.
type HookEnv struct{}

func (h *HookEnv) Manifest() (manifest.Manifest, error) {
	path := os.Getenv(HookedPodManifestEnvVar)
	if path == "" {
		return nil, util.Errorf("No manifest exported")
	}
	return manifest.FromPath(path)
}

func (h *HookEnv) PodID() (types.PodID, error) {
	id := os.Getenv(HookedPodIDEnvVar)
	if id == "" {
		return "", util.Errorf("Did not provide a pod ID to use")
	}

	return types.PodID(id), nil
}

func (h *HookEnv) PodHome() (string, error) {
	path := os.Getenv(HookedPodHomeEnvVar)
	if path == "" {
		return "", util.Errorf("No pod home given for pod")
	}

	return path, nil
}

func (h *HookEnv) Node() (types.NodeName, error) {
	node := os.Getenv(HookedNodeEnvVar)
	if node == "" {
		// TODO: This can't be a hard error right now. It would mean hooks built with this
		// change couldn't run under preparers without this change. We'll fix it later.
		// (At the present time, hooks shouldn't care about their node name.)
		hostname, err := os.Hostname()
		if err != nil {
			return "", util.Errorf("Error getting node name: %v", err)
		}
		node = hostname
	}
	return types.NodeName(node), nil
}

// Initializes a pod from the current_manifest.yaml file in the pod's home directory. This function
// will error or return an old manifest if run during an inappropriate hook event, use of this
// function is discouraged in most cases
func (h *HookEnv) PodFromDisk() (*pods.Pod, error) {
	node, err := h.Node()
	if err != nil {
		return nil, err
	}
	podHome, err := h.PodHome()
	if err != nil {
		return nil, err
	}

	return pods.PodFromPodHome(types.NodeName(node), podHome)
}

// Initializes a pod based on the hooked pod manifest and the system pod root
func (h *HookEnv) Pod() (*pods.Pod, error) {
	factory := pods.NewFactory(os.Getenv(HookedSystemPodRootEnvVar), HookedNodeEnvVar, uri.DefaultFetcher)

	podID, err := h.PodID()
	if err != nil {
		return nil, err
	}

	uuid := types.PodUniqueKey(os.Getenv(HookedPodUniqueKeyEnvVar))
	if uuid == "" {
		return factory.NewLegacyPod(podID), nil
	}
	return factory.NewUUIDPod(podID, uuid)
}

func (h *HookEnv) Config() (*config.Config, error) {
	return config.LoadConfigFile(os.Getenv(HookedConfigPathEnvVar))
}

func (h *HookEnv) Event() (HookType, error) {
	return AsHookType(os.Getenv(HookEventEnvVar))
}

func (h *HookEnv) EnvPath() string {
	return os.Getenv(HookedEnvPathEnvVar)
}

func (h *HookEnv) ConfigDirPath() string {
	return os.Getenv(HookedConfigDirPathEnvVar)
}

func (h *HookEnv) ExitUnlessEvent(types ...HookType) HookType {
	t, _ := h.Event()
	for _, target := range types {
		if t == target {
			return t
		}
	}

	fmt.Printf("this hook responds to %v (but got %s), ignoring", types, t)
	os.Exit(0)
	return HookType("") // never reached
}
