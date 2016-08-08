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

var DEFAULT_PATH = "/usr/local/p2hooks.d"

const (
	HOOK_ENV_VAR                   = "HOOK"
	HOOK_EVENT_ENV_VAR             = "HOOK_EVENT"
	HOOKED_NODE_ENV_VAR            = "HOOKED_NODE"
	HOOKED_POD_ID_ENV_VAR          = "HOOKED_POD_ID"
	HOOKED_POD_HOME_ENV_VAR        = "HOOKED_POD_HOME"
	HOOKED_POD_MANIFEST_ENV_VAR    = "HOOKED_POD_MANIFEST"
	HOOKED_CONFIG_PATH_ENV_VAR     = "HOOKED_CONFIG_PATH"
	HOOKED_ENV_PATH_ENV_VAR        = "HOOKED_ENV_PATH"
	HOOKED_CONFIG_DIR_PATH_ENV_VAR = "HOOKED_CONFIG_DIR_PATH"

	DefaultTimeout = 60 * time.Second
)

type Pod interface {
	ConfigDir() string
	EnvDir() string
	Node() types.NodeName
	Path() string
}

type HookDir struct {
	dirpath string
	logger  *logging.Logger
}

type HookType string

func (hookType HookType) String() string {
	return string(hookType)
}

var (
	BEFORE_INSTALL   = HookType("before_install")
	AFTER_INSTALL    = HookType("after_install") // after_install occurs before we have disabled the old version
	BEFORE_UNINSTALL = HookType("before_uninstall")
	BEFORE_LAUNCH    = HookType("before_launch") // before_launch occurs after we have disabled the old version
	AFTER_LAUNCH     = HookType("after_launch")
	AFTER_AUTH_FAIL  = HookType("after_auth_fail")
)

func AsHookType(value string) (HookType, error) {
	switch value {
	case BEFORE_INSTALL.String():
		return BEFORE_INSTALL, nil
	case AFTER_INSTALL.String():
		return AFTER_INSTALL, nil
	case BEFORE_UNINSTALL.String():
		return BEFORE_UNINSTALL, nil
	case BEFORE_LAUNCH.String():
		return BEFORE_LAUNCH, nil
	case AFTER_LAUNCH.String():
		return AFTER_LAUNCH, nil
	case AFTER_AUTH_FAIL.String():
		return AFTER_AUTH_FAIL, nil
	default:
		return HookType(""), fmt.Errorf("%s is not a valid hook type", value)
	}
}

func Hooks(dirpath string, logger *logging.Logger) *HookDir {
	return &HookDir{dirpath, logger}
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

func (h *HookDir) runHooks(dirpath string, hType HookType, pod Pod, podManifest manifest.Manifest, logger logging.Logger) error {
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
		fmt.Sprintf("%s=%s", HOOK_ENV_VAR, path.Base(dirpath)),
		fmt.Sprintf("%s=%s", HOOK_EVENT_ENV_VAR, hType.String()),
		fmt.Sprintf("%s=%s", HOOKED_NODE_ENV_VAR, pod.Node()),
		fmt.Sprintf("%s=%s", HOOKED_POD_ID_ENV_VAR, podManifest.ID()),
		fmt.Sprintf("%s=%s", HOOKED_POD_HOME_ENV_VAR, pod.Path()),
		fmt.Sprintf("%s=%s", HOOKED_POD_MANIFEST_ENV_VAR, tmpManifestFile.Name()),
		fmt.Sprintf("%s=%s", HOOKED_CONFIG_PATH_ENV_VAR, path.Join(pod.ConfigDir(), configFileName)),
		fmt.Sprintf("%s=%s", HOOKED_ENV_PATH_ENV_VAR, pod.EnvDir()),
		fmt.Sprintf("%s=%s", HOOKED_CONFIG_DIR_PATH_ENV_VAR, pod.ConfigDir()),
	}

	return runDirectory(dirpath, hookEnvironment, logger)
}

func (h *HookDir) RunHookType(hookType HookType, pod Pod, manifest manifest.Manifest) error {
	logger := h.logger.SubLogger(logrus.Fields{
		"pod":      manifest.ID(),
		"pod_path": pod.Path(),
		"event":    hookType.String(),
	})
	logger.NoFields().Infof("Running %s hooks", hookType.String())
	return h.runHooks(h.dirpath, hookType, pod, manifest, logger)
}
