package hooks

import (
	"fmt"
	"os"

	"github.com/square/p2/pkg/config"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

func CurrentEnv() *HookEnv {
	return &HookEnv{}
}

// Hook env is a utility for hook writers using Go. This provides
// useful access to objects exported by the hooks package.
type HookEnv struct{}

func (h *HookEnv) Manifest() (manifest.Manifest, error) {
	path := os.Getenv(HOOKED_POD_MANIFEST_ENV_VAR)
	if path == "" {
		return nil, util.Errorf("No manifest exported")
	}
	return manifest.FromPath(path)
}

func (h *HookEnv) PodID() (types.PodID, error) {
	id := os.Getenv(HOOKED_POD_ID_ENV_VAR)
	if id == "" {
		return "", util.Errorf("Did not provide a pod ID to use")
	}

	return types.PodID(id), nil
}

func (h *HookEnv) PodHome() (string, error) {
	path := os.Getenv(HOOKED_POD_HOME_ENV_VAR)
	if path == "" {
		return "", util.Errorf("No pod home given for pod")
	}

	return path, nil
}

func (h *HookEnv) Node() (types.NodeName, error) {
	node := os.Getenv(HOOKED_NODE_ENV_VAR)
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
	factory := pods.NewFactory(os.Getenv(HOOKED_SYSTEM_POD_ROOT_ENV_VAR), HOOKED_NODE_ENV_VAR)

	podID, err := h.PodID()
	if err != nil {
		return nil, err
	}

	// TODO: pass the pod uuid to hooks when applicable.
	return factory.NewLegacyPod(podID), nil
}

func (h *HookEnv) Config() (*config.Config, error) {
	return config.LoadConfigFile(os.Getenv(HOOKED_CONFIG_PATH_ENV_VAR))
}

func (h *HookEnv) Event() (HookType, error) {
	return AsHookType(os.Getenv(HOOK_EVENT_ENV_VAR))
}

func (h *HookEnv) EnvPath() string {
	return os.Getenv(HOOKED_ENV_PATH_ENV_VAR)
}

func (h *HookEnv) ConfigDirPath() string {
	return os.Getenv(HOOKED_CONFIG_DIR_PATH_ENV_VAR)
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
