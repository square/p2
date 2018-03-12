package p2exec

import (
	"fmt"

	"github.com/square/p2/pkg/types"
)

// DefaultP2Exec is the path to the default p2-exec binary. Specified as a var so you
// can override at build time.
var DefaultP2Exec = "/usr/local/bin/p2-exec"

// Struct that can be used to build an invocation of p2-exec with any
// applicable flags
type P2ExecArgs struct {
	User             string
	EnvDirs          []string
	ExtraEnv         map[string]string
	NoLimits         bool
	PodID            *types.PodID
	CgroupName       string
	CgroupConfigName string
	Command          []string
	WorkDir          string
	RequireFile      string
	ClearEnv         bool

	// LaunchableType defaults to "hoist", so typically it will be set if it's a container
	LaunchableType string

	// ContainerBindMountPaths only should be provided if LaunchableType is "opencontainer". It specifies paths
	// on the host filesystem that should be bind mounted into the container being launched by p2-exec
	ContainerBindMountPaths []string

	// ContainerConfigTemplateFilename is the name of the JSON file that should serve as a template for the opencontainer
	// that will be launched by p2-exec. It should only be set if LaunchableType is "opencontainer"
	ContainerConfigTemplateFilename string
}

func (args P2ExecArgs) CommandLine() []string {
	var cmd []string
	if args.NoLimits {
		cmd = append(cmd, "-n")
	}

	if args.User != "" {
		cmd = append(cmd, "-u", args.User)
	}

	for _, envDir := range args.EnvDirs {
		cmd = append(cmd, "-e", envDir)
	}

	for envVarKey, envVarValue := range args.ExtraEnv {
		cmd = append(cmd, "--extra-env", fmt.Sprintf("%s=%s", envVarKey, envVarValue))
	}

	if args.CgroupConfigName != "" {
		cmd = append(cmd, "-l", args.CgroupConfigName)
	}

	if args.CgroupName != "" {
		cmd = append(cmd, "-c", args.CgroupName)
	}

	if args.WorkDir != "" {
		cmd = append(cmd, "-w", args.WorkDir)
	}

	if args.RequireFile != "" {
		cmd = append(cmd, "--require-file", args.RequireFile)
	}

	if args.ClearEnv {
		cmd = append(cmd, "--clearenv")
	}

	if args.PodID != nil {
		cmd = append(cmd, "--podID", args.PodID.String())
	}

	if args.LaunchableType != "" {
		cmd = append(cmd, "--launchable-type", args.LaunchableType)
	}

	if args.ContainerConfigTemplateFilename != "" {
		cmd = append(cmd, "--container-config-template", args.ContainerConfigTemplateFilename)
	}

	for _, mountPath := range args.ContainerBindMountPaths {
		cmd = append(cmd, "--container-bind-mount-path", mountPath)
	}

	if len(cmd) > 0 {
		cmd = append(cmd, "--")
	}

	return append(cmd, args.Command...)
}
