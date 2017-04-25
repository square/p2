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

func NewContext(dirpath string, podRoot string, logger *logging.Logger, auditLogger AuditLogger) *hookContext {
	return &hookContext{
		dirpath:     dirpath,
		podRoot:     podRoot,
		logger:      logger,
		auditLogger: auditLogger,
	}
}

// runDirectory executes all executable files in a given directory path.
func (h *hookContext) runDirectory(hookEnv *HookExecutionEnvironment) error {
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
		hec := NewHookExecContext(fullpath, f.Name(), DefaultTimeout, *hookEnv, h.logger)
		executable := (f.Mode() & 0111) != 0
		if !executable {
			h.auditLogger.LogFailure(hec, nil)
			h.logger.WithField("path", fullpath).Warnln("Hook is not executable")
			continue
		}
		if f.IsDir() {
			continue
		}

		err := hec.RunWithTimeout()
		if htErr, ok := err.(ErrHookTimeout); ok {
			h.auditLogger.LogFailure(hec, err)
			h.logger.WithErrorAndFields(htErr, logrus.Fields{
				"path":      hec.Path,
				"hook_name": hec.Name,
				"timeout":   hec.Timeout,
			}).Warnln(htErr.Error())
			// we intentionally swallow timeout HookTimeoutErrors
			continue
		} else if err != nil {
			h.auditLogger.LogFailure(hec, err)
			h.logger.WithErrorAndFields(err, logrus.Fields{
				"path":      hec.Path,
				"hook_name": hec.Name,
			}).Warningf("Unknown error in hook %s: %s", hec.Name, err)
			continue
		}
		h.auditLogger.LogSuccess(hec)
	}
	return nil
}

func (h *hookContext) Close() error {
	return h.auditLogger.Close()
}

// RunWithTimeout runs the hook but returns a HookTimeoutError when it exceeds its timeout
//
// Necessary because Run() hangs if the command double-forks without properly
// re-opening its fd's and exec.Start() will dutifully wait on any unclosed fd
//
// NB: in the event of a timeout this will leak descriptors
func (h *HookExecContext) RunWithTimeout() error {
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
func (h *HookExecContext) Run() {
	h.logger.WithField("path", h.Path).Infof("Executing hook %s", h.Name)
	cmd := exec.Command(h.Path)
	hookOut := &bytes.Buffer{}
	cmd.Stdout = hookOut
	cmd.Stderr = hookOut
	cmd.Env = h.env.Env()
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

	hec := &HookExecutionEnvironment{
		HookEnvVar:                path.Base(dirpath),
		HookEventEnvVar:           hType.String(),
		HookedNodeEnvVar:          pod.Node().String(),
		HookedPodIDEnvVar:         podManifest.ID().String(),
		HookedPodHomeEnvVar:       pod.Home(),
		HookedPodManifestEnvVar:   tmpManifestFile.Name(),
		HookedConfigPathEnvVar:    path.Join(pod.ConfigDir(), configFileName),
		HookedEnvPathEnvVar:       pod.ConfigDir(),
		HookedConfigDirPathEnvVar: h.podRoot,
		HookedSystemPodRootEnvVar: pod.UniqueKey().String(),
	}
	return h.runDirectory(hec)
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
