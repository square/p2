package opencontainer

// DefaultRuntimeSpec is the default template for running Linux containers.
var DefaultRuntimeSpec = LinuxRuntimeSpec{
	RuntimeSpec: RuntimeSpec{
		Mounts: map[string]Mount{
			"cgroup": {
				Type:   "cgroup",
				Source: "cgroup",
				Options: []string{
					"nosuid",
					"noexec",
					"nodev",
					"relatime",
				},
			},
			"dev": {
				Type:   "tmpfs",
				Source: "tmpfs",
				Options: []string{
					"nosuid",
					"strictatime",
					"mode=755",
					"size=65536k",
				},
			},
			"devpts": {
				Type:   "devpts",
				Source: "devpts",
				Options: []string{
					"nosuid",
					"noexec",
					"newinstance",
					"ptmxmode=0666",
					"mode=0620",
					"gid=5",
				},
			},
			"mqueue": {
				Type:   "mqueue",
				Source: "mqueue",
				Options: []string{
					"nosuid",
					"noexec",
					"nodev",
				},
			},
			"proc": {
				Type:   "proc",
				Source: "proc",
			},
			"shm": {
				Type:   "tmpfs",
				Source: "shm",
				Options: []string{
					"nosuid",
					"noexec",
					"nodev",
					"mode=1777",
					"size=65536k",
				},
			},
			"sysfs": {
				Type:   "sysfs",
				Source: "sysfs",
				Options: []string{
					"nosuid",
					"noexec",
					"nodev",
				},
			},
		},
		Hooks: Hooks{},
	},
	Linux: LinuxRuntime{
		Resources: &Resources{
			Memory: Memory{
				Swappiness: -1, // "-1" is "inherit from parents", not empty value "0"
			},
		},
		Namespaces: []Namespace{
			{Type: "pid"},
			{Type: "ipc"},
			{Type: "mount"},
		},
		// These are common Linux devices that must be present in the container
		Devices: []Device{
			{
				Path:        "/dev/null",
				Type:        99,
				Major:       1,
				Minor:       3,
				Permissions: "rwm",
				FileMode:    0666,
				UID:         0,
				GID:         0,
			},
			{
				Path:        "/dev/random",
				Type:        99,
				Major:       1,
				Minor:       8,
				Permissions: "rwm",
				FileMode:    0666,
				UID:         0,
				GID:         0,
			},
			{
				Path:        "/dev/full",
				Type:        99,
				Major:       1,
				Minor:       7,
				Permissions: "rwm",
				FileMode:    0666,
				UID:         0,
				GID:         0,
			},
			{
				Path:        "/dev/tty",
				Type:        99,
				Major:       5,
				Minor:       0,
				Permissions: "rwm",
				FileMode:    0666,
				UID:         0,
				GID:         0,
			},
			{
				Path:        "/dev/zero",
				Type:        99,
				Major:       1,
				Minor:       5,
				Permissions: "rwm",
				FileMode:    0666,
				UID:         0,
				GID:         0,
			},
			{
				Path:        "/dev/urandom",
				Type:        99,
				Major:       1,
				Minor:       9,
				Permissions: "rwm",
				FileMode:    0666,
				UID:         0,
				GID:         0,
			},
		},
	},
}
