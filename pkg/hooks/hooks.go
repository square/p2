package hooks

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
)

var DEFAULT_PATH = "/usr/local/p2hooks.d"

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
	BEFORE_INSTALL  = HookType("before_install")
	AFTER_INSTALL   = HookType("after_install")
	AFTER_LAUNCH    = HookType("after_launch")
	AFTER_AUTH_FAIL = HookType("after_auth_fail")
)

func AsHookType(value string) (HookType, error) {
	switch value {
	case BEFORE_INSTALL.String():
		return BEFORE_INSTALL, nil
	case AFTER_INSTALL.String():
		return AFTER_INSTALL, nil
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
	if err != nil {
		return err
	}

	for _, f := range entries {
		fullpath := path.Join(dirpath, f.Name())
		executable := (f.Mode() & 0111) != 0
		if !executable {
			logger.WithField("path", fullpath).Warnln("Could not execute hook - file is not executable")
			continue
		}
		cmd := exec.Command(fullpath)
		hookOut := &bytes.Buffer{}
		cmd.Stdout = hookOut
		cmd.Stderr = hookOut
		cmd.Env = environment
		err := cmd.Run()
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err":    err,
				"path":   fullpath,
				"output": hookOut.String(),
			}).Warnln("Could not execute hook")
		} else {
			logger.WithFields(logrus.Fields{
				"path":   fullpath,
				"output": hookOut.String(),
			}).Infoln("Executed hook")
		}
	}

	return nil
}

func (h *HookDir) runHooks(dirpath string, pod Pod, podManifest *pods.PodManifest) error {

	logger := h.logger.SubLogger(logrus.Fields{
		"hook":     dirpath,
		"pod":      podManifest.ID(),
		"pod_path": pod.Path(),
	})

	configFileName, err := podManifest.ConfigFileName()
	if err != nil {
		return err
	}

	// Write manifest to a file so hooks can read it.
	tmpManifestFile, err := ioutil.TempFile("", fmt.Sprintf("%s-manifest.yaml", podManifest.Id))
	if err != nil {
		logger.WithField("err", err).Warnln("Unable to open manifest file for hooks")
		return err
	}
	defer os.Remove(tmpManifestFile.Name())

	err = podManifest.Write(tmpManifestFile)
	if err != nil {
		logger.WithField("err", err).Warnln("Unable to write manifest file for hooks")
		return err
	}

	hookEnvironment := []string{
		fmt.Sprintf("HOOK=%s", path.Base(dirpath)),
		fmt.Sprintf("HOOKED_POD_ID=%s", podManifest.Id),
		fmt.Sprintf("HOOKED_POD_HOME=%s", pod.Path()),
		fmt.Sprintf("HOOKED_POD_MANIFEST=%s", tmpManifestFile.Name()),
		fmt.Sprintf("HOOKED_CONFIG_PATH=%s", path.Join(pod.ConfigDir(), configFileName)),
		fmt.Sprintf("HOOKED_ENV_PATH=%s", pod.EnvDir()),
	}

	return runDirectory(dirpath, hookEnvironment, logger)
}

func (h *HookDir) RunHookType(hookType HookType, pod Pod, manifest *pods.PodManifest) error {
	dirpath := path.Join(h.dirpath, hookType.String())
	return h.runHooks(dirpath, pod, manifest)
}
