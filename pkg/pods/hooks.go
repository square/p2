package pods

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
)

func runDirectory(dirpath string, environment []string) error {
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

func RunHooks(dirpath string, podId string) error {
	hookEnvironment := os.Environ()
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("POD_ID=%s", podId))
	hookEnvironment = append(hookEnvironment, fmt.Sprintf("CONFIG_PATH=%s", ConfigDir(podId)))

	return runDirectory(dirpath, hookEnvironment)
}
