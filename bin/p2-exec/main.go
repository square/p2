package main

import (
	// #include <sys/resource.h>
	"C"

	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v2"
)

var (
	username = kingpin.Flag("user", "The user to execute as.").Short('u').String()
	envDir   = kingpin.Flag("env",
		"A directory of env files to add to the environment. May be specified more than once. In the case of conflicting variable names, the directory appearing last will win.",
	).Short('e').Strings()
	extraEnv = kingpin.Flag(
		"extra-env",
		"Specifies an extra environment KEY=VALUE pair to set for the process. May be used multiple times. Takes precedence over --env if there are conflicts",
	).StringMap()
	launchableName = kingpin.Flag("launchable", "The key in $PLATFORM_CONFIG_PATH containing the cgroup parameters.").Short('l').String()
	cgroupName     = kingpin.Flag("cgroup", "The name of the cgroup that should be created.").Short('c').String()
	nolim          = kingpin.Flag("nolimit", "Remove rlimits.").Short('n').Bool()
	clearEnv       = kingpin.Flag("clearenv", "Clear all environment variables before loading envDir(s).").Bool()
	workDir        = kingpin.Flag("workdir", "Set working directory.").Short('w').String()
	umask          = kingpin.Flag("umask", "Set the process umask. Use octal notation ex. 0022").Short('m').Default(umaskDefault).String()
	umaskDefault   = ""

	cmd = kingpin.Arg("command", "the command to execute").Required().Strings()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	if *umask != umaskDefault {
		effectiveUmask, err := strconv.ParseInt(*umask, 8, 0)
		if err != nil {
			log.Fatalf("umask not expressed in octal notation. %v\n", err)
		}
		unix.Umask(int(effectiveUmask))
	}

	if *clearEnv {
		os.Clearenv()
	}

	for _, dir := range *envDir {
		err := loadEnvDir(dir)
		if err != nil {
			log.Fatal(err)
		}
	}

	for envKey, envValue := range *extraEnv {
		err := os.Setenv(envKey, envValue)
		if err != nil {
			log.Fatal(err)
		}
	}

	if *nolim {
		err := nolimit()
		if err != nil {
			log.Fatal(err)
		}
	}

	if *launchableName == "" && *cgroupName != "" {
		log.Fatalf("Specified cgroup name %q, but no launchable name was specified", *cgroupName)
	}
	if *launchableName != "" && *cgroupName == "" {
		log.Fatalf("Specified launchable name %q, but no cgroup name was specified", *launchableName)
	}
	if *launchableName != "" && *cgroupName != "" {
		if platconf := os.Getenv("PLATFORM_CONFIG_PATH"); platconf != "" {
			err := cgEnter(platconf, *launchableName, *cgroupName)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("No PLATFORM_CONFIG_PATH, cannot determine cgroup")
		}
	}

	if *username != "" {
		err := changeUser(*username)
		if err != nil {
			log.Fatal(err)
		}
	}

	if *workDir != "" {
		err := os.Chdir(*workDir)
		if err != nil {
			log.Fatal(err)
		}
	}

	binPath, err := exec.LookPath((*cmd)[0])
	if err != nil {
		log.Fatal(err)
	}

	/*
		from `man setpgid`:
		* "If pid is zero, then the call applies to the current process" / "if pid is 0, the process ID of the calling process shall be used"
		* "Also, if pgid is 0, the process ID of the indicated process shall be used."
		So we set the current process's process group ID to be equal to that of its process ID.
		Without this, its process group ID is equal to its parent's.
		This means that operations that act on their entire process tree will affect not only their own pod,
		but all pods on the same machine (in a typical setup, where they all originate from the same parent).

		For example, a renice may change the entire parent process tree's nice level.
		Worse, this new nice level may propagate into a container, causing the container to be unable to set its nice level back to 0.
		This has been observed to cause containers to fail to start.
	*/
	if err := unix.Setpgid(0, 0); err != nil {
		log.Fatal(err)
	}

	err = syscall.Exec(binPath, *cmd, os.Environ())
	// should never be reached
	if err != nil {
		log.Fatalf("Error executing command %q: %s", *cmd, err)
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
			return util.Errorf("Could not read %q: %s", filepath.Join(dir, envFile.Name()), err)
		}
		enval := string(envBytes)

		// unset a variable if the file is empty
		if len(enval) == 0 {
			err = os.Unsetenv(envFile.Name())
			if err != nil {
				return util.Errorf("Could not unsetenv %q: %s", envFile.Name(), err)
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
			return util.Errorf("Could not setenv %q to %q: %s", envFile.Name(), enval, err)
		}
	}

	return nil
}

// requires a path to a platform configuration file in this format:
// <launchablename>:
//   cgroup:
//     cpus: 4
//     memory: 123456
// and the <launchablename> and <cgroupname>
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
		return util.Errorf("Could not find cgroupfs mount point: %s", err)
	}
	err = cg.Write(cgConfig)
	if _, ok := err.(cgroups.UnsupportedError); ok {
		// if a subsystem is not supported, just log
		// and carry on
		log.Printf("Unsupported subsystem (%s), continuing\n", err)
		return nil
	} else if err != nil {
		return util.Errorf("Could not set cgroup parameters: %s", err)
	}
	return cg.AddPID(cgConfig.Name, 0)
}

// generalized code to remove rlimits on both darwin and linux
func nolimit() error {
	maxFDs, err := sysMaxFDs()
	if err != nil {
		return util.Errorf("Could not determine max FDs on system: %s", err)
	}

	ret, err := C.setrlimit(C.RLIMIT_NOFILE, maxFDs)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_NOFILE (max FDs %v): %s", maxFDs, err)
	}

	unlimit := sysUnRlimit()
	ret, err = C.setrlimit(C.RLIMIT_CPU, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_CPU: %s", err)
	}
	ret, err = C.setrlimit(C.RLIMIT_DATA, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_DATA: %s", err)
	}
	ret, err = C.setrlimit(C.RLIMIT_FSIZE, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_FSIZE: %s", err)
	}

	ret, err = C.setrlimit(C.RLIMIT_MEMLOCK, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_MEMLOCK: %s", err)
	}
	ret, err = C.setrlimit(C.RLIMIT_NPROC, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_NPROC: %s", err)
	}
	ret, err = C.setrlimit(C.RLIMIT_RSS, unlimit)
	if ret != 0 && err != nil {
		return util.Errorf("Could not set RLIMIT_RSS: %s", err)
	}
	return nil
}
