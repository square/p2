package hooks

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
)

var DEFAULT_PATH = "/usr/local/p2hooks.d"

const (
	HOOK_ENV_VAR                = "HOOK"
	HOOK_EVENT_ENV_VAR          = "HOOK_EVENT"
	HOOKED_POD_ID_ENV_VAR       = "HOOKED_POD_ID"
	HOOKED_POD_HOME_ENV_VAR     = "HOOKED_POD_HOME"
	HOOKED_POD_MANIFEST_ENV_VAR = "HOOKED_POD_MANIFEST"
	HOOKED_CONFIG_PATH_ENV_VAR  = "HOOKED_CONFIG_PATH"
	HOOKED_ENV_PATH_ENV_VAR     = "HOOKED_ENV_PATH"
)

type Pod interface {
	ConfigDir() string
	EnvDir() string
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
		logger.WithField("path", fullpath).Infof("Executing hook %s", f.Name())
		cmd := exec.Command(fullpath)
		hookOut := &bytes.Buffer{}
		cmd.Stdout = hookOut
		cmd.Stderr = hookOut
		cmd.Env = environment
		err := cmd.Run()
		if err != nil {
			logger.WithErrorAndFields(err, logrus.Fields{
				"path":   fullpath,
				"output": hookOut.String(),
			}).Warnf("Could not execute hook %s", f.Name())
		} else {
			logger.WithFields(logrus.Fields{
				"path":   fullpath,
				"output": hookOut.String(),
			}).Debugln("Executed hook")
		}
	}

	return nil
}

func (h *HookDir) runHooks(dirpath string, hType HookType, pod Pod, podManifest pods.Manifest, logger logging.Logger) error {
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
		fmt.Sprintf("%s=%s", HOOKED_POD_ID_ENV_VAR, podManifest.ID()),
		fmt.Sprintf("%s=%s", HOOKED_POD_HOME_ENV_VAR, pod.Path()),
		fmt.Sprintf("%s=%s", HOOKED_POD_MANIFEST_ENV_VAR, tmpManifestFile.Name()),
		fmt.Sprintf("%s=%s", HOOKED_CONFIG_PATH_ENV_VAR, path.Join(pod.ConfigDir(), configFileName)),
		fmt.Sprintf("%s=%s", HOOKED_ENV_PATH_ENV_VAR, pod.EnvDir()),
	}

	return runDirectory(dirpath, hookEnvironment, logger)
}

func (h *HookDir) RunHookType(hookType HookType, pod Pod, manifest pods.Manifest) error {
	logger := h.logger.SubLogger(logrus.Fields{
		"pod":      manifest.ID(),
		"pod_path": pod.Path(),
		"event":    hookType.String(),
	})
	logger.NoFields().Infof("Running %s hooks", hookType.String())
	return h.runHooks(h.dirpath, hookType, pod, manifest, logger)
}
