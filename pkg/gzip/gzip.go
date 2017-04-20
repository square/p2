package gzip

import (
	"os"
	"os/exec"
	"os/user"
	"syscall"

	p2user "github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
)

// ExtractTarGz extracts the specified tarball to the specified destination,
// as the specified user.
func ExtractTarGz(owner string, filename string, dest string) (err error) {
	ownerUID, ownerGID, err := p2user.IDs(owner)
	if err != nil {
		return err
	}

	currentUser, err := user.Current()
	if err != nil {
		return err
	}

	err = util.MkdirChownAll(dest, ownerUID, ownerGID, 0755)
	if err != nil {
		return util.Errorf("error creating root directory %s: %s", dest, err)
	}
	err = os.Chown(dest, ownerUID, ownerGID)
	if err != nil {
		return util.Errorf("error setting ownership of root directory %s: %s", dest, err)
	}

	cmd := exec.Command("tar", "xpzf", filename, "-C", dest)
	if currentUser.Username != owner {
		// If we are running as a non-root user (e.g. in tests), don't change user.
		// Non-root users are understandably not allowed to change to other users...
		// not even themselves.
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Credential: &syscall.Credential{Uid: uint32(ownerUID), Gid: uint32(ownerGID)},
		}
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return util.Errorf("error extracting: %v %s", err, string(output))
	}
	return nil
}
