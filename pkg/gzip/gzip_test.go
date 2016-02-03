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

func TestFileWithoutDir(t *testing.T) {
	tarPath := util.From(runtime.Caller(0)).ExpandPath("file_without_dir.tar.gz")
	file, err := os.Open(tarPath)
	Assert(t).IsNil(err, "expected no error opening file")

	tmpdir, err := ioutil.TempDir("", "gziptest")
	defer os.RemoveAll(tmpdir)
	Assert(t).IsNil(err, "expected no error creating tempdir")

	dest := filepath.Join(tmpdir, "dest")
	err = os.Mkdir(dest, 0755)
	Assert(t).IsNil(err, "expected no error creating destdir")

	user, err := user.Current()
	Assert(t).IsNil(err, "expected no error getting current user")

	err = ExtractTarGz(user.Username, file, dest)
	Assert(t).IsNil(err, "expected no error extracting tarball")

	_, err = os.Stat(filepath.Join(dest, "a", "b"))
	Assert(t).IsNil(err, "expected no error statting extracted file")
}
