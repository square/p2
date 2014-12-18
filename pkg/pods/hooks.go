package pods

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
)

type HookDir struct {
	dirpath string
}

func Hooks(dirpath string) *HookDir {
	return &HookDir{dirpath}
}

func runDirectory(dirpath string, environment []string, pod *Pod) error {
	entries, err := ioutil.ReadDir(dirpath)
	if err != nil {
		return err
	}

	for _, f := range entries {
		fullpath := path.Join(dirpath, f.Name())
		executable := (f.Mode() & 0111) != 0
		if !executable {
			pod.logError(fmt.Errorf("%s is not executable", fullpath), "Could not execute hook")
			continue
		}
		cmd := exec.Command(fullpath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = environment
		err := cmd.Run()
		if err != nil {
			pod.logError(err, "Could not execute hook")
		}
	}

	return nil
}

func runHooks(dirpath string, pod *Pod, podManifest *PodManifest) error {
	configFileName, err := podManifest.ConfigFileName()
	if err != nil {
		return err
	}

	// Write manifest to a file so hooks can read it.
	tmpManifestFile, err := ioutil.TempFile("", fmt.Sprintf("%s-manifest.yaml", podManifest.Id))
	if err != nil {
		pod.logError(err, "Unable to open manifest file for hooks")
		return err
	}
	defer os.Remove(tmpManifestFile.Name())

	err = podManifest.Write(tmpManifestFile)
	if err != nil {
		pod.logError(err, "Unable to write manifest file for hooks")
		return err
	}

	hookEnvironment := os.Environ()
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("POD_ID=%s", podManifest.Id))
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("POD_HOME=%s", PodPath(podManifest.Id)))
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("POD_MANIFEST=%s", tmpManifestFile.Name()))
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("CONFIG_PATH=%s", path.Join(pod.ConfigDir(), configFileName)))
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("ENV_PATH=%s", pod.EnvDir()))

	return runDirectory(dirpath, hookEnvironment, pod)
}

func (h *HookDir) RunBefore(pod *Pod, manifest *PodManifest) error {
	return runHooks(path.Join(h.dirpath, "before"), pod, manifest)
}

func (h *HookDir) RunAfter(pod *Pod, manifest *PodManifest) error {
	return runHooks(path.Join(h.dirpath, "after"), pod, manifest)
}
