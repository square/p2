package gzip

import (
	"io/ioutil"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/util"
)

func testExtraction(t *testing.T, tarfile string,
	check func(error, string),
) {
	tarfile = path.Join("testdata", tarfile) // prefix with testdata so this is ignored by downstream dep management
	tarPath := util.From(runtime.Caller(0)).ExpandPath(tarfile)

	tmpdir, err := ioutil.TempDir("", "gziptest")
	defer os.RemoveAll(tmpdir)
	Assert(t).IsNil(err, "expected no error creating tempdir")

	dest := filepath.Join(tmpdir, "dest")
	err = os.Mkdir(dest, 0755)
	Assert(t).IsNil(err, "expected no error creating destdir")

	user, err := user.Current()
	Assert(t).IsNil(err, "expected no error getting current user")

	err = ExtractTarGz(user.Username, tarPath, dest)

	check(err, dest)
}

func TestFileWithoutDir(t *testing.T) {
	testExtraction(t, "file_without_dir.tar.gz", func(tarErr error, dest string) {
		Assert(t).IsNil(tarErr, "expected no error extracting tarball")

		_, err := os.Stat(filepath.Join(dest, "a", "b"))
		Assert(t).IsNil(err, "expected no error statting extracted file")
	})
}

func TestSelfHardlink(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("bsdtar doesn't handle a selflink tarball")
	}

	testExtraction(t, "file_with_selflink.tar.gz", func(tarErr error, dest string) {
		Assert(t).IsNil(tarErr, "expected no error extracting tarball")

		_, err := os.Stat(filepath.Join(dest, "a"))
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
