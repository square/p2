package gzip

import (
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/util"
)

func testExtractionWithSetup(t *testing.T, tarfile string,
	setup func(string) error,
	check func(error, string),
) {
	tarPath := util.From(runtime.Caller(0)).ExpandPath(tarfile)
	file, err := os.Open(tarPath)
	Assert(t).IsNil(err, "expected no error opening file")

	tmpdir, err := ioutil.TempDir("", "gziptest")
	defer os.RemoveAll(tmpdir)
	Assert(t).IsNil(err, "expected no error creating tempdir")

	dest := filepath.Join(tmpdir, "dest")
	err = os.Mkdir(dest, 0755)
	Assert(t).IsNil(err, "expected no error creating destdir")

	err = setup(dest)
	Assert(t).IsNil(err, "expected no error performing test-specific setup")

	user, err := user.Current()
	Assert(t).IsNil(err, "expected no error getting current user")

	realDest, err := filepath.EvalSymlinks(dest)
	Assert(t).IsNil(err, "expected no error evaluating symlinks")

	err = ExtractTarGz(user.Username, file, realDest)

	check(err, realDest)
}

func testExtraction(t *testing.T, tarfile string, check func(error, string)) {
	testExtractionWithSetup(t, tarfile, func(string) error { return nil }, check)
}

func TestFileWithoutDir(t *testing.T) {
	testExtraction(t, "file_without_dir.tar.gz", func(tarErr error, dest string) {
		Assert(t).IsNil(tarErr, "expected no error extracting tarball")

		_, err := os.Stat(filepath.Join(dest, "a", "b", "c"))
		Assert(t).IsNil(err, "expected no error statting extracted file")
	})
}

func TestPathWithParent(t *testing.T) {
	testExtraction(t, "path_with_parent.tar.gz", func(tarErr error, dest string) {
		Assert(t).IsNotNil(tarErr, "expected error extracting tarball")

		_, err := os.Stat(filepath.Join(dest, "..", "b"))
		Assert(t).IsTrue(os.IsNotExist(err), "expected extracted file not to exist")
	})
}

func testSymlink(t *testing.T, linkTarget func(string) string, check func(error, string)) {
	testExtractionWithSetup(t, "file_without_dir.tar.gz",
		func(dest string) error {
			return os.Symlink(linkTarget(dest), filepath.Join(dest, "a"))
		},
		check,
	)
}

func testEscapingSymlink(t *testing.T, linkTarget func(string) string) {
	testSymlink(t, linkTarget, func(tarErr error, dest string) {
		Assert(t).IsNotNil(tarErr, "expected error extracting tarball")

		_, err := os.Stat(filepath.Join(dest, "..", "b", "c"))
		Assert(t).IsTrue(os.IsNotExist(err), "expected extracted file not to exist")
	})
}

func testNonEscapingSymlink(t *testing.T, linkTarget func(string) string) {
	testSymlink(t, linkTarget, func(tarErr error, dest string) {
		Assert(t).IsNil(tarErr, "expected no error extracting tarball")

		_, err := os.Stat(filepath.Join(dest, "b", "c"))
		Assert(t).IsNil(err, "expected no error statting extracted file")
	})
}

func TestEscapingAbsoluteSymlink(t *testing.T) {
	testEscapingSymlink(t, func(dest string) string {
		return filepath.Dir(dest)
	})
}

func TestEscapingAbsoluteSymlink2(t *testing.T) {
	testEscapingSymlink(t, func(dest string) string {
		return "/"
	})
}

func TestEscapingRelativeSymlink(t *testing.T) {
	testEscapingSymlink(t, func(_dest string) string {
		return ".."
	})
}

func TestNonEscapingAbsoluteSymlink(t *testing.T) {
	testNonEscapingSymlink(t, func(dest string) string {
		return dest
	})
}

func TestNonEscapingRelativeSymlink(t *testing.T) {
	testNonEscapingSymlink(t, func(_dest string) string {
		return "."
	})
}
