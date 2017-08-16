package hooks

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"

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
		if err != nil {
			if htErr, ok := err.(ErrHookTimeout); ok {
				h.auditLogger.LogFailure(hec, err)
				h.logger.WithErrorAndFields(htErr, logrus.Fields{
					"path":      hec.Path,
					"hook_name": hec.Name,
					"timeout":   hec.Timeout,
				}).Warnln(htErr.Error())
			}

			// we don't need to log anything for non-timeout errors because they're already logged from Run()
			return err
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
	ctx, cancel := context.WithTimeout(context.Background(), h.Timeout)
	defer cancel()
	finished := make(chan error)
	go func() {
		defer close(finished)
		select {
		case finished <- h.Run():
		case <-ctx.Done():
			// This means the hook timed out and therefore nothing is listening
			// to the finished channel
		}
	}()

	select {
	case err := <-finished:
		return err
	case <-ctx.Done():
		return ErrHookTimeout{*h}
	}
}

// Run executes the hook in the context of its environment and logs the output
func (h *HookExecContext) Run() error {
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
		return err
	}
	h.logger.WithFields(logrus.Fields{
		"path":   h.Path,
		"output": hookOut.String(),
	}).Debugln("Executed hook")

	return nil
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
		HookedEnvPathEnvVar:       pod.EnvDir(),
		HookedConfigDirPathEnvVar: pod.ConfigDir(),
		HookedSystemPodRootEnvVar: h.podRoot,
		HookedPodUniqueKeyEnvVar:  pod.UniqueKey().String(),
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
