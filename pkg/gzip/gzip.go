package gzip

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
)

// ExtractTarGz reads a gzipped tar stream and extracts all files to the destination
// directory. If an owner name is specified, all files will be created to be owned by that
// user; otherwise, the tar specifies ownership.
//
// If any file would be extracted outside of the destination directory due to relative paths
// containing '..', the archive will be rejected with an error.
func ExtractTarGz(owner string, fp io.Reader, dest string) (err error) {
	fz, err := gzip.NewReader(fp)
	if err != nil {
		return util.Errorf("error reading gzip data: %s", err)
	}
	defer fz.Close()
	tr := tar.NewReader(fz)

	var ownerUID, ownerGID int
	if owner != "" {
		ownerUID, ownerGID, err = user.IDs(owner)
		if err != nil {
			return err
		}
	}

	err = util.MkdirChownAll(dest, ownerUID, ownerGID, 0755)
	if err != nil {
		return util.Errorf("error creating root directory %s: %s", dest, err)
	}
	err = os.Chown(dest, ownerUID, ownerGID)
	if err != nil {
		return util.Errorf("error setting ownership of root directory %s: %s", dest, err)
	}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return util.Errorf("read error: %s", err)
		}
		fpath := filepath.Join(dest, hdr.Name)
		var uid, gid int
		if owner == "" {
			uid, gid = hdr.Uid, hdr.Gid
		} else {
			uid, gid = ownerUID, ownerGID
		}

		// Error on all files that would end up outside the destination directory.
		if !strings.HasPrefix(fpath, dest) {
			return util.Errorf(
				"cannot extract %s, as its target %s is outside the root directory %s",
				hdr.Name,
				fpath,
				dest,
			)
		}

		parent := filepath.Dir(fpath)
		if err := util.MkdirChownAll(parent, ownerUID, ownerGID, 0755); err != nil {
			return util.Errorf(
				"error creating directory %s (parent of %s): %s",
				parent,
				hdr.Name,
				err,
			)
		}

		switch hdr.Typeflag {
		case tar.TypeSymlink:
			err = os.Symlink(hdr.Linkname, fpath)
			if err != nil {
				return util.Errorf(
					"error creating symlink %s -> %s: %s",
					fpath,
					hdr.Linkname,
					err,
				)
			}
			err = os.Lchown(fpath, uid, gid)
			if err != nil {
				return util.Errorf("error setting owner of %s: %s", fpath, err)
			}
		case tar.TypeLink:
			// hardlink paths are encoded relative to the tarball root, rather than
			// the path of the link itself, so we need to resolve that path
			linkTarget, err := filepath.Rel(filepath.Dir(hdr.Name), hdr.Linkname)
			if err != nil {
				return util.Errorf(
					"error resolving link: %s -> %s: %s",
					fpath,
					hdr.Linkname,
					err,
				)
			}
			// we can't make the hardlink right away because the target might not
			// exist, so we'll just make a symlink instead
			err = os.Symlink(linkTarget, fpath)
			if err != nil {
				return util.Errorf(
					"error creating symlink %s -> %s (originally hardlink): %s",
					fpath,
					linkTarget,
					err,
				)
			}
		case tar.TypeDir:
			err = os.Mkdir(fpath, hdr.FileInfo().Mode())
			if err != nil && !os.IsExist(err) {
				return util.Errorf("error creating directory %s: %s", fpath, err)
			}

			err = os.Chown(fpath, uid, gid)
			if err != nil {
				return util.Errorf("error setting ownership of %s: %s", fpath, err)
			}
		case tar.TypeReg, tar.TypeRegA:
			// Extract the file inside a closure to limit the scope of its open FD
			err = func() (innerErr error) {
				f, err := os.OpenFile(
					fpath,
					os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
					hdr.FileInfo().Mode(),
				)
				if err != nil {
					return util.Errorf("error creating %s: %s", fpath, err)
				}
				// Released at end of "case" statement
				defer func() {
					if closeErr := f.Close(); innerErr == nil {
						innerErr = closeErr
					}
				}()

				err = f.Chown(uid, gid)
				if err != nil {
					return util.Errorf("error setting file ownership of %s: %s", fpath, err)
				}

				_, err = io.Copy(f, tr)
				if err != nil {
					return util.Errorf("error extracting to %s: %s", fpath, err)
				}
				return nil
			}()
			if err != nil {
				return err
			}
		default:
			return util.Errorf("unhandled type flag %q (header %v)", hdr.Typeflag, hdr)
		}
	}
	return nil
}
