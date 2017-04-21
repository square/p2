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
)

func NewContext(dirpath string, podRoot string, logger *logging.Logger) *hookContext {
	return &hookContext{
		dirpath: dirpath,
		podRoot: podRoot,
		logger:  logger,
	}
}

// runDirectory executes all executable files in a given directory path.
func (h *hookContext) runDirectory(hookEnv []string) error {
	// func runDirectory(dirpath string, hookEnv []string, logger logging.Logger) error {
	entries, err := ioutil.ReadDir(h.dirpath)
	if os.IsNotExist(err) {
		h.logger.WithField("dir", h.dirpath).Debugln("Hooks not set up")
		return nil
	}
	if err != nil {
		return err
	}

	for _, f := range entries {
		fullpath := path.Join(h.dirpath, f.Name())
		executable := (f.Mode() & 0111) != 0
		if !executable {
			h.logger.WithField("path", fullpath).Warnln("Hook is not executable")
			continue
		}
		if f.IsDir() {
			continue
		}

		h := NewHookExecContext(fullpath, f.Name(), DefaultTimeout, hookEnv, h.logger)

		err := h.RunWithTimeout()
		if htErr, ok := err.(ErrHookTimeout); ok {
			h.logger.WithErrorAndFields(htErr, logrus.Fields{
				"path":      h.Path,
				"hook_name": h.Name,
				"timeout":   h.Timeout,
			}).Warnln(htErr.Error())
			// we intentionally swallow timeout HookTimeoutErrors
		} else if err != nil {
			h.logger.WithErrorAndFields(err, logrus.Fields{
				"path":      h.Path,
				"hook_name": h.Name,
			}).Warningf("Unknown error in hook %s: %s", h.Name, err)
		}
	}

	return nil
}

// RunWithTimeout runs the hook but returns a HookTimeoutError when it exceeds its timeout
//
// Necessary because Run() hangs if the command double-forks without properly
// re-opening its fd's and exec.Start() will dutifully wait on any unclosed fd
//
// NB: in the event of a timeout this will leak descriptors
func (h *hookExecContext) RunWithTimeout() error {
	finished := make(chan struct{})
	go func() {
		h.Run()
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(h.Timeout):
		return ErrHookTimeout{*h}
	}

	return nil
}

// Run executes the hook in the context of its environment and logs the output
func (h *hookExecContext) Run() {
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

func (h *hookContext) runHooks(dirpath string, hType HookType, pod Pod, podManifest manifest.Manifest, logger logging.Logger) error {
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

	return h.runDirectory(hookEnvironment)
}

func (h *hookContext) RunHookType(hookType HookType, pod Pod, manifest manifest.Manifest) error {
	logger := h.logger.SubLogger(logrus.Fields{
		"pod":      manifest.ID(),
		"pod_path": pod.Home(),
		"event":    hookType.String(),
	})
	logger.NoFields().Infof("Running %s hooks", hookType.String())
	return h.runHooks(h.dirpath, hookType, pod, manifest, logger)
}
