package hooks

import (
	"fmt"
	"os"

	"github.com/square/p2/pkg/config"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

func CurrentEnv() *HookEnv {
	return &HookEnv{}
}

// Hook env is a utility for hook writers using Go. This provides
// useful access to objects exported by the hooks package.
type HookEnv struct{}

func (h *HookEnv) Manifest() (pods.Manifest, error) {
	path := os.Getenv("HOOKED_POD_MANIFEST")
	if path == "" {
		return nil, util.Errorf("No manifest exported")
	}
	return pods.ManifestFromPath(path)
}

func (h *HookEnv) Pod() (*pods.Pod, error) {
	id := os.Getenv("HOOKED_POD_ID")
	if id == "" {
		return nil, util.Errorf("Did not provide a pod ID to use")
	}
	path := os.Getenv("HOOKED_POD_HOME")
	if path == "" {
		return nil, util.Errorf("No pod home given for pod ID %s", id)
	}

	return pods.NewPod(id, path), nil
}

func (h *HookEnv) Config() (*config.Config, error) {
	return config.LoadConfigFile(os.Getenv("HOOKED_CONFIG_PATH"))
}

func (h *HookEnv) Event() (HookType, error) {
	return AsHookType(os.Getenv("HOOK_EVENT"))
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
