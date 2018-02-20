package opencontainer

import (
	"os"
	"strings"

	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/util"
)

type RuncConfig struct {
	// NoNewKeyring decides if the --no-new-keyring flag should be added to "runc
	// run" invocations. This is necessary for kernels that do not have support
	// for this feature yet
	NoNewKeyring bool

	// Root tells runc where to store container state, which should be in a tmpfs
	// mount. By default it stores state in /run, but on systems where /run
	// doesn't exist it should be overridden
	Root string
}

// GetConfig() inspects the local system and returns an appropriate RuncConfig
func GetConfig(osVersionDetector osversion.Detector) (RuncConfig, error) {
	var config RuncConfig
	_, osVersion, err := osVersionDetector.Version()
	if err != nil {
		return RuncConfig{}, util.Errorf("could not detect OS version to determine runc configuration: %s", err)
	}

	if strings.HasPrefix(osVersion.String(), "6.") {
		config.NoNewKeyring = true
	}

	runDir, err := os.Stat("/run")
	switch {
	case os.IsNotExist(err):
		devShmDir, err := os.Stat("/dev/shm")
		if err != nil {
			return config, util.Errorf("error checking that /dev/shm exists and is a directory to determine runc configuration: %s", err)
		}

		if !devShmDir.IsDir() {
			return config, util.Errorf("/dev/shm is not a directory, could not determine runc configuration to use")
		}
		config.Root = "/dev/shm/runc"
	case err != nil:
		return config, util.Errorf("could not stat() /run to determine if it exists: %s", err)
	case err == nil:
		if !runDir.IsDir() {
			return config, util.Errorf("/run exists but is not a dir, could not determine runc configuration to use")
		}

		// leave the default Root (should be /run/runc)
	}

	return config, nil
}
