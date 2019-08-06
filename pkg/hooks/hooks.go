package hooks

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
)

// RunHookResult holds the hook run result data
type RunHookResult struct {
	Err error
}

func NewContext(dirpath string, podRoot string, logger *logging.Logger, auditLogger AuditLogger) *hookContext {
	return &hookContext{
		dirpath:     dirpath,
		podRoot:     podRoot,
		logger:      logger,
		auditLogger: auditLogger,
	}
}

// runDirectory executes all executable files in a given directory path.
func (h *hookContext) runDirectory(
	hookEnv *HookExecutionEnvironment,
	logger logging.Logger,
	hooksRequired []string,
) error {
	entries, err := ioutil.ReadDir(h.dirpath)
	if os.IsNotExist(err) {
		logger.WithField("dir", h.dirpath).Debugln("Hooks not set up")
		return nil
	}
	if err != nil {
		return err
	}

	for _, f := range entries {
		fullpath := path.Join(h.dirpath, f.Name())
		hec := NewHookExecContext(fullpath, f.Name(), DefaultTimeout, *hookEnv, logger)
		executable := (f.Mode() & 0111) != 0
		if !executable {
			h.auditLogger.LogFailure(hec, nil)
			logger.WithField("path", fullpath).Warnln("Hook is not executable")
			continue
		}
		if f.IsDir() {
			continue
		}

		logger := logger.SubLogger(logrus.Fields{
			"path":      hec.Path,
			"hook_name": hec.Name,
		})

		err := hec.RunWithTimeout(logger)
		if htErr, ok := err.(ErrHookTimeout); ok {
			h.auditLogger.LogFailure(hec, err)
			logger.WithErrorAndFields(htErr, logrus.Fields{
				"timeout": hec.Timeout,
			}).Warnln(htErr.Error())
			// we intentionally swallow timeout HookTimeoutErrors
			continue
		} else if err != nil {
			h.auditLogger.LogFailure(hec, err)

			// check against a list of required hooks to see if we should return err
			for _, reqHook := range hooksRequired {
				if hec.Name == reqHook {
					logger.WithError(err).Errorf("Fatal error in hook %s: %s", hec.Name, err)
					return err
				}
			}

			logger.WithError(err).Warningf("Unknown error in hook %s: %s", hec.Name, err)
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
func (h *HookExecContext) RunWithTimeout(logger logging.Logger) error {
	finished := make(chan RunHookResult)

	go func() {
		defer close(finished)
		err := h.Run(logger)
		finished <- RunHookResult{err}
	}()

	select {
	case f := <-finished:
		if f.Err != nil {
			return f.Err
		}
	case <-time.After(h.Timeout):
		return ErrHookTimeout{*h}
	}

	return nil
}

// Run executes the hook in the context of its environment and logs the output
func (h *HookExecContext) Run(logger logging.Logger) error {
	logger.Infof("Executing hook %s", h.Name)
	cmd := exec.Command(h.Path)
	hookOut := &bytes.Buffer{}
	cmd.Stdout = hookOut
	cmd.Stderr = hookOut
	cmd.Env = h.env.Env()
	err := cmd.Run()
	if err != nil {
		logger.WithErrorAndFields(err, logrus.Fields{
			"output": hookOut.String(),
		}).Warnf("Could not execute hook %s", h.Name)

		return err
	}

	logger.WithFields(logrus.Fields{
		"output": hookOut.String(),
	}).Debugln("Executed hook")

	return nil
}

func (h *hookContext) runHooks(
	dirpath string,
	hType HookType,
	pod Pod,
	podManifest manifest.Manifest,
	logger logging.Logger,
	hooksRequired []string,
) error {
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

	podManifest.SetReadOnlyIfUnset(pod.ReadOnly())

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
		HookedPodReadOnly:         strconv.FormatBool(podManifest.GetReadOnly()),
	}
	return h.runDirectory(hec, logger, hooksRequired)
}

func (h *hookContext) RunHookType(hookType HookType, pod Pod, manifest manifest.Manifest, hooksRequired []string) error {
	logger := h.logger.SubLogger(logrus.Fields{
		"pod":      manifest.ID(),
		"pod_path": pod.Home(),
		"event":    hookType.String(),
	})
	logger.NoFields().Infof("Running %s hooks", hookType.String())
	return h.runHooks(h.dirpath, hookType, pod, manifest, logger, hooksRequired)
}
