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
	// PreparerInit hooks run during the initialization of the preparer
	PreparerInit = HookType("preparer_init")
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
	case PreparerInit.String():
		return PreparerInit, nil
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
	HookedPodReadOnly         = "HOOKED_POD_READ_ONLY"

	DefaultTimeout = 120 * time.Second
)

// Pod is the minimum set of functions needed for a hook to operate on a Pod
type Pod interface {
	ConfigDir() string
	EnvDir() string
	Node() types.NodeName
	Home() string
	UniqueKey() types.PodUniqueKey
	ReadOnly() bool
}

type hookContext struct {
	dirpath     string
	podRoot     string
	logger      *logging.Logger
	auditLogger AuditLogger
}

// The set of environment variables exposed to the hook as it runs
// TODO Need this be public?
type HookExecutionEnvironment struct {
	HookEnvVar,
	HookEventEnvVar,
	HookedNodeEnvVar,
	HookedPodIDEnvVar,
	HookedPodHomeEnvVar,
	HookedPodManifestEnvVar,
	HookedConfigPathEnvVar,
	HookedEnvPathEnvVar,
	HookedConfigDirPathEnvVar,
	HookedSystemPodRootEnvVar,
	HookedPodUniqueKeyEnvVar,
	HookedPodReadOnly string
}

// The set of UNIX environment variables for the hook's execution
func (hee *HookExecutionEnvironment) Env() []string {
	return []string{
		fmt.Sprintf("%s=%s", HookEnvVar, hee.HookEnvVar),
		fmt.Sprintf("%s=%s", HookEventEnvVar, hee.HookEventEnvVar),
		fmt.Sprintf("%s=%s", HookedNodeEnvVar, hee.HookedNodeEnvVar),
		fmt.Sprintf("%s=%s", HookedPodIDEnvVar, hee.HookedPodIDEnvVar),
		fmt.Sprintf("%s=%s", HookedPodHomeEnvVar, hee.HookedPodHomeEnvVar),
		fmt.Sprintf("%s=%s", HookedPodManifestEnvVar, hee.HookedPodManifestEnvVar),
		fmt.Sprintf("%s=%s", HookedConfigPathEnvVar, hee.HookedConfigPathEnvVar),
		fmt.Sprintf("%s=%s", HookedEnvPathEnvVar, hee.HookedEnvPathEnvVar),
		fmt.Sprintf("%s=%s", HookedConfigDirPathEnvVar, hee.HookedConfigDirPathEnvVar),
		fmt.Sprintf("%s=%s", HookedSystemPodRootEnvVar, hee.HookedSystemPodRootEnvVar),
		fmt.Sprintf("%s=%s", HookedPodUniqueKeyEnvVar, hee.HookedPodUniqueKeyEnvVar),
		fmt.Sprintf("%s=%s", HookedPodReadOnly, hee.HookedPodReadOnly),
	}
}

// AuditLogger defines a mechanism for logging hook success or failure to a store, such as a file or SQLite
type AuditLogger interface {
	// LogSuccess should be invoked on successful events.
	LogSuccess(env *HookExecContext)
	// LogFailure should be called in case of error or timeout. The second parameter may be null.
	LogFailure(env *HookExecContext, err error)

	// Close any references held by this AuditLogger
	Close() error
}

type HookExecContext struct {
	Path        string // path to hook's executable
	Name        string // human-readable name of Hook
	Timeout     time.Duration
	env         HookExecutionEnvironment // This will be used as the set of UNIX environment variables for the hook's execution
	logger      logging.Logger
	auditLogger AuditLogger
}

func NewHookExecContext(path string, name string, timeout time.Duration, env HookExecutionEnvironment, logger logging.Logger) *HookExecContext {
	return &HookExecContext{
		Path:    path,
		Name:    name,
		Timeout: timeout,
		env:     env,
		logger:  logger,
	}
}

// Close any references held by this HookExecContext
func (hec *HookExecContext) Close() {
	if err := hec.auditLogger.Close(); err != nil {
		hec.logger.WithError(err).Errorln("Caught and ignored error closing an audit logger.")
	}
}

// ErrHookTimeout is returned when a Hook's execution times out
type ErrHookTimeout struct {
	Hook HookExecContext
}

func (e ErrHookTimeout) Error() string {
	sec := e.Hook.Timeout / time.Millisecond
	return fmt.Sprintf("Hook %s timed out after %dms", e.Hook.Name, sec)
}
