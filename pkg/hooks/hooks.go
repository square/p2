package hooks

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
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
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = environment
		err := cmd.Run()
		if err != nil {
			logger.WithFields(logrus.Fields{
				"err":  err,
				"path": fullpath,
			}).Warnln("Could not execute hook")
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

	hookEnvironment := os.Environ()
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("POD_ID=%s", podManifest.Id))
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("POD_HOME=%s", pod.Path()))
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("POD_MANIFEST=%s", tmpManifestFile.Name()))
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("CONFIG_PATH=%s", path.Join(pod.ConfigDir(), configFileName)))
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("ENV_PATH=%s", pod.EnvDir()))

	return runDirectory(dirpath, hookEnvironment, logger)
}

func (h *HookDir) RunBeforeInstall(pod Pod, manifest *pods.PodManifest) error {
	return h.runHooks(path.Join(h.dirpath, "before_install"), pod, manifest)
}

func (h *HookDir) RunAfterLaunch(pod Pod, manifest *pods.PodManifest) error {
	return h.runHooks(path.Join(h.dirpath, "after_launch"), pod, manifest)
}
