package p2exec

// DefaultP2Exec is the path to the default p2-exec binary. Specified as a var so you
// can override at build time.
var DefaultP2Exec = "/usr/local/bin/p2-exec"

// Struct that can be used to build an invocation of p2-exec with any
// applicable flags
type P2ExecArgs struct {
	User             string
	EnvDirs          []string
	NoLimits         bool
	CgroupName       string
	CgroupConfigName string
	Command          []string
	WorkDir          string
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

	if args.CgroupConfigName != "" {
		cmd = append(cmd, "-l", args.CgroupConfigName)
	}

	if args.CgroupName != "" {
		cmd = append(cmd, "-c", args.CgroupName)
	}

	if args.WorkDir != "" {
		cmd = append(cmd, "-w", args.WorkDir)
	}

	if len(cmd) > 0 {
		cmd = append(cmd, "--")
	}

	return append(cmd, args.Command...)
}
