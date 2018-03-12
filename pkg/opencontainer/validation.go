package opencontainer

import (
	"path/filepath"

	"github.com/square/p2/pkg/util"
)

// ValidateSpec enforces constraints P2 imposes on a config.json before
// running it.
func ValidateSpec(spec *Spec, uid int, gid int) error {
	if spec == nil {
		return util.Errorf("nil spec")
	}

	if spec.Solaris != nil || spec.Windows != nil {
		return util.Errorf("unsupported platform, only \"linux\" is supported")
	}

	if spec.Root == nil {
		return util.Errorf("a root filesystem must be specified in config.json")
	}
	if filepath.Base(spec.Root.Path) != spec.Root.Path {
		return util.Errorf("invalid container root: %s", spec.Root.Path)
	}
	if !spec.Root.Readonly {
		return util.Errorf("root filesystem is required to be set to read only in config.json")
	}

	user := spec.Process.User
	if uid != int(user.UID) || gid != int(user.GID) {
		return util.Errorf("cannot execute as (%d:%d): container expects %d:%d",
			uid, gid, user.UID, user.GID)
	}

	capabilities := spec.Process.Capabilities
	if user.UID != 0 && capabilities != nil {
		if len(capabilities.Bounding) > 0 ||
			len(capabilities.Effective) > 0 ||
			len(capabilities.Inheritable) > 0 ||
			len(capabilities.Permitted) > 0 ||
			len(capabilities.Ambient) > 0 {
			return util.Errorf("capabilities were present in config.json but are not allowed")
		}
	}

	if !spec.Process.NoNewPrivileges {
		return util.Errorf("noNewPrivileges must be set to true in config.json")
	}

	return nil
}
