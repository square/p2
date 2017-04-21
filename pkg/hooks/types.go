package hooks

import (
	"fmt"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/types"
)

// HookType represents the stage in the Hook lifecycle
type HookType string

func (hookType HookType) String() string {
	return string(hookType)
}

var (
	// BeforeInstall hooks run before the artifact is downloaded and extracted
	BeforeInstall = HookType("before_install")
	// AfterInstall hooks run after we have installed but before we have disabled the old version
	AfterInstall = HookType("after_install")
	// BeforeUninstall happens after shutdown but before uninstall
	BeforeUninstall = HookType("before_uninstall")
	// BeforeLaunch occurs after we have disabled the old version
	BeforeLaunch = HookType("before_launch")
	// AfterLaunch occurs after launch
	AfterLaunch = HookType("after_launch")
	// AfterAuth occurs conditionally when artifact authorization fails
	AfterAuthFail = HookType("after_auth_fail")
)

func AsHookType(value string) (HookType, error) {
	switch value {
	case BeforeInstall.String():
		return BeforeInstall, nil
	case AfterInstall.String():
		return AfterInstall, nil
	case BeforeUninstall.String():
		return BeforeUninstall, nil
	case BeforeLaunch.String():
		return BeforeLaunch, nil
	case AfterLaunch.String():
		return AfterLaunch, nil
	case AfterAuthFail.String():
		return AfterAuthFail, nil
	default:
		return HookType(""), fmt.Errorf("%s is not a valid hook type", value)
	}
}

// DefaultPath is a directory on disk where hooks are installed by default.
var DefaultPath = "/usr/local/p2hooks.d"

const (
	HookEnvVar                = "HOOK"
	HookEventEnvVar           = "HOOK_EVENT"
	HookedNodeEnvVar          = "HOOKED_NODE"
	HookedPodIDEnvVar         = "HOOKED_POD_ID"
	HookedPodHomeEnvVar       = "HOOKED_POD_HOME"
	HookedPodManifestEnvVar   = "HOOKED_POD_MANIFEST"
	HookedConfigPathEnvVar    = "HOOKED_CONFIG_PATH"
	HookedEnvPathEnvVar       = "HOOKED_ENV_PATH"
	HookedConfigDirPathEnvVar = "HOOKED_CONFIG_DIR_PATH"
	HookedSystemPodRootEnvVar = "HOOKED_SYSTEM_POD_ROOT"
	HookedPodUniqueKeyEnvVar  = "HOOKED_POD_UNIQUE_KEY"

	DefaultTimeout = 120 * time.Second
)

// Pod is the minimum set of functions needed for a hook to operate on a Pod
type Pod interface {
	ConfigDir() string
	EnvDir() string
	Node() types.NodeName
	Home() string
	UniqueKey() types.PodUniqueKey
}

type hookContext struct {
	dirpath string
	podRoot string
	logger  *logging.Logger
}

type hookExecContext struct {
	Path    string // path to hook's executable
	Name    string // human-readable name of Hook
	Timeout time.Duration
	env     []string // unix environment in which the hook should be executed
	logger  *logging.Logger
}

func NewHookExecContext(path string, name string, timeout time.Duration, env []string, logger *logging.Logger) *hookExecContext {
	return &hookExecContext{
		Path:    path,
		Name:    name,
		Timeout: timeout,
		env:     env,
		logger:  logger,
	}
}

// ErrHookTimeout is returned when a Hook's execution times out
type ErrHookTimeout struct {
	Hook hookExecContext
}

func (e ErrHookTimeout) Error() string {
	sec := e.Hook.Timeout / time.Millisecond
	return fmt.Sprintf("Hook %s timed out after %dms", e.Hook.Name, sec)
}
