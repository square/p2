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
	Envs             []string
	ExtraEnv         map[string]string
	NoLimits         bool
	PodID            *types.PodID
	CgroupName       string
	CgroupConfigName string
	Command          []string
	WorkDir          string
	RequireFile      string
	ClearEnv         bool
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

	for _, env := range args.Envs {
		cmd = append(cmd, "env", env)
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

	if len(cmd) > 0 {
		cmd = append(cmd, "--")
	}

	return append(cmd, args.Command...)
}
