package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v1"
	"gopkg.in/yaml.v2"
)

var (
	username       = kingpin.Flag("user", "the user to execute as").Short('u').String()
	envDir         = kingpin.Flag("env", "a directory of env files to add to the environment").Short('e').String()
	launchableName = kingpin.Flag("launchable", "the key in $PLATFORM_CONFIG_PATH containing the cgroup parameters").Short('l').String()
	cgroupName     = kingpin.Flag("cgroup", "the name of the cgroup that should be created").Short('c').String()
	nolim          = kingpin.Flag("nolimit", "remove rlimits").Short('n').Bool()
	clearEnv       = kingpin.Flag("clearenv", "clear all environment variables before loading envDir").Bool()

	cmd = kingpin.Arg("command", "the command to execute").Required().Strings()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	if *clearEnv {
		os.Clearenv()
	}

	if *envDir != "" {
		err := loadEnvDir(*envDir)
		if err != nil {
			log.Fatal(err)
		}
	}

	if *launchableName == "" && *cgroupName != "" {
		log.Fatal("Specified cgroup name %q, but no launchable name was specified", *cgroupName)
	}
	if *launchableName != "" && *cgroupName == "" {
		log.Fatal("Specified launchable name %q, but no cgroup name was specified", *launchableName)
	}
	if platconf := os.Getenv("PLATFORM_CONFIG_PATH"); platconf != "" && *launchableName != "" && *cgroupName != "" {
		err := cgEnter(platconf, *launchableName, *cgroupName)
		if err != nil {
			log.Fatal(err)
		}
	}

	binPath, err := exec.LookPath((*cmd)[0])
	if err != nil {
		log.Fatal(err)
	}

	err = rlimDropExec(*username, binPath, *cmd)
	// unreachable
	if err != nil {
		log.Fatal(err)
	}
}

// loadEnvDir iterates over all files in the given directory and loads them as
// environment variables, matching the behavior of chpst -e or envdir.
func loadEnvDir(dir string) error {
	envFiles, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, envFile := range envFiles {
		if envFile.IsDir() {
			continue
		}
		if strings.IndexByte(envFile.Name(), '=') != -1 {
			return util.Errorf("env file %q cannot contain equals sign in its filename", filepath.Join(dir, envFile.Name()))
		}
		envBytes, err := ioutil.ReadFile(filepath.Join(dir, envFile.Name()))
		if err != nil {
			return err
		}
		enval := string(envBytes)

		// unset a variable if the file is empty
		if len(enval) == 0 {
			err = os.Unsetenv(envFile.Name())
			if err != nil {
				return err
			}
			continue
		}

		// take only the first line
		if index := strings.IndexByte(enval, '\n'); index != -1 {
			enval = enval[:index]
		}
		// discard trailing space
		enval = strings.TrimRight(enval, " \t")
		// replace nulls with newlines
		enval = strings.Map(func(c rune) rune {
			if c == 0 {
				return '\n'
			}
			return c
		}, enval)

		err = os.Setenv(envFile.Name(), enval)
		if err != nil {
			return err
		}
	}

	return nil
}

// requires a path to a platform configuration file in this format:
// <launchablename>:
//   cgroup:
//     cpus: 4
//     memory: 123456
// and a cgroup name specification in this format: "<launchablename>:<cgroupname>"
// a cgroup with the name <cgroupname> will be created, using the parameters for
// <launchablename> found in the platform configuration
// then, the current PID will be added to that cgroup
func cgEnter(platconf, launchableName, cgroupName string) error {
	platconfBuf, err := ioutil.ReadFile(platconf)
	if err != nil {
		return err
	}
	cgMap := make(map[string]map[string]cgroups.Config)
	err = yaml.Unmarshal(platconfBuf, cgMap)
	if err != nil {
		return err
	}

	if _, ok := cgMap[launchableName]; !ok {
		return util.Errorf("Unknown launchable %q in PLATFORM_CONFIG_PATH", launchableName)
	}
	if _, ok := cgMap[launchableName]["cgroup"]; !ok {
		return util.Errorf("Launchable %q has malformed PLATFORM_CONFIG_PATH", launchableName)
	}
	cgConfig := cgMap[launchableName]["cgroup"]
	cgConfig.Name = cgroupName

	cg, err := cgroups.Find()
	if err != nil {
		return err
	}
	err = cg.Write(cgConfig)
	if err != nil {
		return err
	}
	return cg.AddPID(cgConfig.Name, os.Getpid())
}
