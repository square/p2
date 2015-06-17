package gzip

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"

	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
)

func ExtractTarGz(owner string, fp io.Reader, fpName string, dest string) (err error) {
	fz, err := gzip.NewReader(fp)
	if err != nil {
		return util.Errorf("Unable to create gzip reader: %s", err)
	}
	defer fz.Close()

	tr := tar.NewReader(fz)
	uid, gid, err := user.IDs(owner)
	if err != nil {
		return err
	}

	err = util.MkdirChownAll(dest, uid, gid, 0755)
	if err != nil {
		return util.Errorf(
			"Unable to create root directory %s when unpacking %s: %s",
			dest,
			fpName,
			err,
		)
	}
	err = os.Chown(dest, uid, gid)
	if err != nil {
		return util.Errorf(
			"Unable to chown root directory %s to %s when unpacking %s: %s",
			dest,
			owner,
			fpName,
			err,
		)
	}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return util.Errorf("Unable to read %s: %s", fpName, err)
		}
		fpath := filepath.Join(dest, hdr.Name)

		switch hdr.Typeflag {
		case tar.TypeSymlink:
			err = os.Symlink(hdr.Linkname, fpath)
			if err != nil {
				return util.Errorf(
					"Unable to create destination symlink %s (to %s) when unpacking %s: %s",
					fpath,
					hdr.Linkname,
					fpName,
					err,
				)
			}
		case tar.TypeLink:
			// hardlink paths are encoded relative to the tarball root, rather than
			// the path of the link itself, so we need to resolve that path
			linkTarget, err := filepath.Rel(filepath.Dir(hdr.Name), hdr.Linkname)
			if err != nil {
				return util.Errorf(
					"Unable to resolve relative path for hardlink %s (to %s) when unpacking %s: %s",
					fpath,
					hdr.Linkname,
					fpName,
					err,
				)
			}
			// we can't make the hardlink right away because the target might not
			// exist, so we'll just make a symlink instead
			err = os.Symlink(linkTarget, fpath)
			if err != nil {
				return util.Errorf(
					"Unable to create destination symlink %s (resolved %s to %s) when unpacking %s: %s",
					fpath,
					linkTarget,
					hdr.Linkname,
					fpName,
					err,
				)
			}
		case tar.TypeDir:
			err = os.Mkdir(fpath, hdr.FileInfo().Mode())
			if err != nil && !os.IsExist(err) {
				return util.Errorf(
					"Unable to create destination directory %s when unpacking %s: %s",
					fpath,
					fpName,
					err,
				)
			}

			err = os.Chown(fpath, uid, gid)
			if err != nil {
				return util.Errorf(
					"Unable to chown destination directory %s to %s when unpacking %s: %s",
					fpath,
					owner,
					fpName,
					err,
				)
			}
		case tar.TypeReg, tar.TypeRegA:
			f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, hdr.FileInfo().Mode())
			if err != nil {
				return util.Errorf(
					"Unable to open destination file %s when unpacking %s: %s",
					fpath,
					fpName,
					err,
				)
			}
			defer f.Close()

			err = f.Chown(uid, gid) // this operation may cause tar unpacking to become significantly slower. Refactor as necessary.
			if err != nil {
				return util.Errorf(
					"Unable to chown destination file %s to %s when unpacking %s: %s",
					fpath,
					owner,
					fpName,
					err,
				)
			}

			_, err = io.Copy(f, tr)
			if err != nil {
				return util.Errorf(
					"Unable to copy into destination file %s when unpacking %s: %s",
					fpath,
					fpName,
					err,
				)
			}
			f.Close() // eagerly release file descriptors rather than letting them pile up
		default:
			return util.Errorf(
				"Unhandled type flag %q (header %v) when unpacking %s",
				hdr.Typeflag,
				hdr,
				fpName,
			)
		}
	}
	return nil
}
