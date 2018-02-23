package opencontainer

import "os"

func int64ToPointer(i int64) *int64 {
	return &i
}

func fileModeToPointer(fileMode os.FileMode) *os.FileMode {
	return &fileMode
}

func uint32ToPointer(u uint32) *uint32 {
	return &u
}

// DefaultRuntimeSpec is the default template for running Linux containers.
// NOTE: runtime.json was removed from the opencontainer spec, and this var is
// not referenced.  For now it's recommended that opencontainer launchables
// provide their full configuration in a packaged config.json file in their
// artifacts. This variable is being left for now for historical reference
var DefaultRuntimeSpec = Spec{
	Mounts: []Mount{
		{
			Destination: "cgroup",
			Type:        "cgroup",
			Source:      "cgroup",
			Options: []string{
				"nosuid",
				"noexec",
				"nodev",
				"relatime",
			},
		},
		{
			Destination: "dev",
			Type:        "tmpfs",
			Source:      "tmpfs",
			Options: []string{
				"nosuid",
				"strictatime",
				"mode=755",
				"size=65536k",
			},
		},
		{
			Destination: "devpts",
			Type:        "devpts",
			Source:      "devpts",
			Options: []string{
				"nosuid",
				"noexec",
				"newinstance",
				"ptmxmode=0666",
				"mode=0620",
				"gid=5",
			},
		},
		{
			Destination: "mqueue",
			Type:        "mqueue",
			Source:      "mqueue",
			Options: []string{
				"nosuid",
				"noexec",
				"nodev",
			},
		},
		{
			Destination: "proc",
			Type:        "proc",
			Source:      "proc",
		},
		{
			Destination: "shm",
			Type:        "tmpfs",
			Source:      "shm",
			Options: []string{
				"nosuid",
				"noexec",
				"nodev",
				"mode=1777",
				"size=65536k",
			},
		},
		{
			Destination: "sysfs",
			Type:        "sysfs",
			Source:      "sysfs",
			Options: []string{
				"nosuid",
				"noexec",
				"nodev",
			},
		},
	},
	Hooks: &Hooks{},
	Linux: &Linux{
		Resources: &LinuxResources{
			Memory: &LinuxMemory{
				Swap: int64ToPointer(-1), // "-1" is "inherit from parents", not empty value "0"
			},
		},
		Namespaces: []LinuxNamespace{
			{Type: "pid"},
			{Type: "ipc"},
			{Type: "mount"},
		},
		// These are common Linux devices that must be present in the container
		Devices: []LinuxDevice{
			{
				Path:     "/dev/null",
				Type:     "c",
				Major:    1,
				Minor:    3,
				FileMode: fileModeToPointer(0666),
				UID:      uint32ToPointer(0),
				GID:      uint32ToPointer(0),
			},
			{
				Path:     "/dev/random",
				Type:     "c",
				Major:    1,
				Minor:    8,
				FileMode: fileModeToPointer(0666),
				UID:      uint32ToPointer(0),
				GID:      uint32ToPointer(0),
			},
			{
				Path:     "/dev/full",
				Type:     "c",
				Major:    1,
				Minor:    7,
				FileMode: fileModeToPointer(0666),
				UID:      uint32ToPointer(0),
				GID:      uint32ToPointer(0),
			},
			{
				Path:     "/dev/tty",
				Type:     "c",
				Major:    5,
				Minor:    0,
				FileMode: fileModeToPointer(0666),
				UID:      uint32ToPointer(0),
				GID:      uint32ToPointer(0),
			},
			{
				Path:     "/dev/zero",
				Type:     "c",
				Major:    1,
				Minor:    5,
				FileMode: fileModeToPointer(0666),
				UID:      uint32ToPointer(0),
				GID:      uint32ToPointer(0),
			},
			{
				Path:     "/dev/urandom",
				Type:     "c",
				Major:    1,
				Minor:    9,
				FileMode: fileModeToPointer(0666),
				UID:      uint32ToPointer(0),
				GID:      uint32ToPointer(0),
			},
		},
	},
}
