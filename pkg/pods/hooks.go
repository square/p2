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

func (h *HookDir) runDirectory(dirpath string, environment []string) error {
	entries, err := ioutil.ReadDir(dirpath)
	if err != nil {
		return err
	}

	for _, f := range entries {
		fullpath := path.Join(dirpath, f.Name())
		executable := (f.Mode() & 0111) != 0
		if !executable {
			// TODO: Port to structured logger.
			fmt.Printf("%s is not executable\n", f.Name())
			continue
		}
		cmd := exec.Command(fullpath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = environment
		err := cmd.Run()
		if err != nil {
			// TODO: Port to structured logger.
			fmt.Println(err)
		}
	}

	return nil
}

func (h *HookDir) runHooks(dirpath string, pod *Pod, podManifest *PodManifest) error {
	configFileName, err := podManifest.configFileName()
	if err != nil {
		return err
	}

	hookEnvironment := os.Environ()
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("POD_ID=%s", podManifest.Id))
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("CONFIG_PATH=%s", path.Join(pod.ConfigDir(), configFileName)))

	return h.runDirectory(dirpath, hookEnvironment)
}

func (h *HookDir) RunBefore(pod *Pod, manifest *PodManifest) error {
	return h.runHooks(path.Join(h.dirpath, "before"), pod, manifest)
}

func (h *HookDir) RunAfter(pod *Pod, manifest *PodManifest) error {
	return h.runHooks(path.Join(h.dirpath, "after"), pod, manifest)
}
