package hooks

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
)

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

type hookDir struct {
	dirpath string
	podRoot string
	logger  *logging.Logger
}

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

func Hooks(dirpath string, podRoot string, logger *logging.Logger) *hookDir {
	return &hookDir{
		dirpath: dirpath,
		podRoot: podRoot,
		logger:  logger,
	}
}

// runDirectory executes all executable files in a given directory path.
func runDirectory(dirpath string, environment []string, logger logging.Logger) error {
	entries, err := ioutil.ReadDir(dirpath)
	if os.IsNotExist(err) {
		logger.WithField("dir", dirpath).Debugln("Hooks not set up")
		return nil
	}
	if err != nil {
		return err
	}

	for _, f := range entries {
		fullpath := path.Join(dirpath, f.Name())
		executable := (f.Mode() & 0111) != 0
		if !executable {
			logger.WithField("path", fullpath).Warnln("Hook is not executable")
			continue
		}
		if f.IsDir() {
			continue
		}

		h := Hook{fullpath, f.Name(), DefaultTimeout, environment, logger}

		err := h.RunWithTimeout()
		if htErr, ok := err.(HookTimeoutError); ok {
			logger.WithErrorAndFields(htErr, logrus.Fields{
				"path":      h.Path,
				"hook_name": h.Name,
				"timeout":   h.Timeout,
			}).Warnln(htErr.Error())
			// we intentionally swallow timeout HookTimeoutErrors
		} else if err != nil {
			logger.WithErrorAndFields(err, logrus.Fields{
				"path":      h.Path,
				"hook_name": h.Name,
			}).Warningf("Unknown error in hook %s: %s", h.Name, err)
		}
	}

	return nil
}

type Hook struct {
	Path    string // path to hook's executable
	Name    string // human-readable name of Hook
	Timeout time.Duration
	env     []string // unix environment in which the hook should be executed
	logger  logging.Logger
}

// HookTimeoutError is returned when a Hook's execution times out
type HookTimeoutError struct {
	Hook Hook
}

func (e HookTimeoutError) Error() string {
	sec := e.Hook.Timeout / time.Millisecond
	return fmt.Sprintf("Hook %s timed out after %dms", e.Hook.Name, sec)
}

// RunWithTimeout runs the hook but returns a HookTimeoutError when it exceeds its timeout
//
// Necessary because Run() hangs if the command double-forks without properly
// re-opening its fd's and exec.Start() will dutifully wait on any unclosed fd
//
// NB: in the event of a timeout this will leak descriptors
func (h *Hook) RunWithTimeout() error {
	finished := make(chan struct{})
	go func() {
		h.Run()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(h.Timeout):
		return HookTimeoutError{*h}
	}

	return nil
}

// Run executes the hook in the context of its environment and logs the output
func (h *Hook) Run() {
	h.logger.WithField("path", h.Path).Infof("Executing hook %s", h.Name)
	cmd := exec.Command(h.Path)
	hookOut := &bytes.Buffer{}
	cmd.Stdout = hookOut
	cmd.Stderr = hookOut
	cmd.Env = h.env
	err := cmd.Run()
	if err != nil {
		h.logger.WithErrorAndFields(err, logrus.Fields{
			"path":   h.Path,
			"output": hookOut.String(),
		}).Warnf("Could not execute hook %s", h.Name)
	} else {
		h.logger.WithFields(logrus.Fields{
			"path":   h.Path,
			"output": hookOut.String(),
		}).Debugln("Executed hook")
	}
}

func (h *hookDir) runHooks(dirpath string, hType HookType, pod Pod, podManifest manifest.Manifest, logger logging.Logger) error {
	configFileName, err := podManifest.ConfigFileName()
	if err != nil {
		return err
	}

	// Write manifest to a file so hooks can read it.
	tmpManifestFile, err := ioutil.TempFile("", fmt.Sprintf("%s-manifest.yaml", podManifest.ID()))
	if err != nil {
		logger.WithErrorAndFields(err, logrus.Fields{
			"dir": dirpath,
		}).Warnln("Unable to open manifest file for hooks")
		return err
	}
	defer os.Remove(tmpManifestFile.Name())

	err = podManifest.Write(tmpManifestFile)
	if err != nil {
		logger.WithErrorAndFields(err, logrus.Fields{
			"dir": dirpath,
		}).Warnln("Unable to write manifest file for hooks")
		return err
	}

	hookEnvironment := []string{
		fmt.Sprintf("%s=%s", HookEnvVar, path.Base(dirpath)),
		fmt.Sprintf("%s=%s", HookEventEnvVar, hType.String()),
		fmt.Sprintf("%s=%s", HookedNodeEnvVar, pod.Node()),
		fmt.Sprintf("%s=%s", HookedPodIDEnvVar, podManifest.ID()),
		fmt.Sprintf("%s=%s", HookedPodHomeEnvVar, pod.Home()),
		fmt.Sprintf("%s=%s", HookedPodManifestEnvVar, tmpManifestFile.Name()),
		fmt.Sprintf("%s=%s", HookedConfigPathEnvVar, path.Join(pod.ConfigDir(), configFileName)),
		fmt.Sprintf("%s=%s", HookedEnvPathEnvVar, pod.EnvDir()),
		fmt.Sprintf("%s=%s", HookedConfigDirPathEnvVar, pod.ConfigDir()),
		fmt.Sprintf("%s=%s", HookedSystemPodRootEnvVar, h.podRoot),
		fmt.Sprintf("%s=%s", HookedPodUniqueKeyEnvVar, pod.UniqueKey()),
	}

	return runDirectory(dirpath, hookEnvironment, logger)
}

func (h *hookDir) RunHookType(hookType HookType, pod Pod, manifest manifest.Manifest) error {
	logger := h.logger.SubLogger(logrus.Fields{
		"pod":      manifest.ID(),
		"pod_path": pod.Home(),
		"event":    hookType.String(),
	})
	logger.NoFields().Infof("Running %s hooks", hookType.String())
	return h.runHooks(h.dirpath, hookType, pod, manifest, logger)
}
