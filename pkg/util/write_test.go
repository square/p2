package util

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
)

func TestWriteNewFile(t *testing.T) {
	tmp, err := ioutil.TempDir("", "write")
	Assert(t).IsNil(err, "should have created tmpdir")
	defer os.RemoveAll(tmp)

	write, err := WriteIfChanged(filepath.Join(tmp, "tmp"), []byte("tmp"), 0600)
	Assert(t).IsNil(err, "should have written")
	Assert(t).IsTrue(write, "should have written")

	info, err := os.Stat(filepath.Join(tmp, "tmp"))
	Assert(t).IsNil(err, "should have statted new file")
	Assert(t).IsTrue(info.Mode() == 0600, "should have had 0600 permissions")

	content, err := ioutil.ReadFile(filepath.Join(tmp, "tmp"))
	Assert(t).IsNil(err, "should have read new file")
	Assert(t).IsTrue(bytes.Equal(content, []byte("tmp")), "should have written correct contents")
}

func TestNewFileZeroPerms(t *testing.T) {
	tmp, err := ioutil.TempDir("", "write")
	Assert(t).IsNil(err, "should have created tmpdir")
	defer os.RemoveAll(tmp)

	write, err := WriteIfChanged(filepath.Join(tmp, "tmp"), []byte("tmp"), 0)
	Assert(t).IsNil(err, "should have written")
	Assert(t).IsTrue(write, "should have written")

	info, err := os.Stat(filepath.Join(tmp, "tmp"))
	Assert(t).IsNil(err, "should have statted new file")
	Assert(t).IsTrue(info.Mode() != 0, "should not have had 0 permissions")
}

func TestWriteUnchangedFile(t *testing.T) {
	tmp, err := ioutil.TempDir("", "write")
	Assert(t).IsNil(err, "should have created tmpdir")
	defer os.RemoveAll(tmp)

	err = ioutil.WriteFile(filepath.Join(tmp, "tmp"), []byte("tmp"), 0600)
	Assert(t).IsNil(err, "should have written file initially")

	info, err := os.Stat(filepath.Join(tmp, "tmp"))
	Assert(t).IsNil(err, "should have statted new file")

	write, err := WriteIfChanged(filepath.Join(tmp, "tmp"), []byte("tmp"), 0600)
	Assert(t).IsNil(err, "should have written without error")
	Assert(t).IsFalse(write, "should not have actually written")

	info2, err := os.Stat(filepath.Join(tmp, "tmp"))
	Assert(t).IsNil(err, "should have statted new file second time")
	Assert(t).IsTrue(info.ModTime() == info2.ModTime(), "should not have modified file")
}

func TestUpdatePermissions(t *testing.T) {
	tmp, err := ioutil.TempDir("", "write")
	Assert(t).IsNil(err, "should have created tmpdir")
	defer os.RemoveAll(tmp)

	err = ioutil.WriteFile(filepath.Join(tmp, "tmp"), []byte("tmp"), 0600)
	Assert(t).IsNil(err, "should have written file initially")

	write, err := WriteIfChanged(filepath.Join(tmp, "tmp"), []byte("tmp"), 0644)
	Assert(t).IsNil(err, "should have written without error")
	Assert(t).IsTrue(write, "should have actually chmodded")

	info, err := os.Stat(filepath.Join(tmp, "tmp"))
	Assert(t).IsNil(err, "should have statted new file second time")
	Assert(t).IsTrue(info.Mode() == 0644, "should have changed to mode 0644")
}

func TestNoUpdatePermissionsDuringWrite(t *testing.T) {
	tmp, err := ioutil.TempDir("", "write")
	Assert(t).IsNil(err, "should have created tmpdir")
	defer os.RemoveAll(tmp)

	err = ioutil.WriteFile(filepath.Join(tmp, "tmp"), []byte("tmp"), 0600)
	Assert(t).IsNil(err, "should have written file initially")

	_, err = WriteIfChanged(filepath.Join(tmp, "tmp"), []byte("tmp2"), 0)
	Assert(t).IsNil(err, "should have written without error")

	info, err := os.Stat(filepath.Join(tmp, "tmp"))
	Assert(t).IsNil(err, "should have statted new file")
	Assert(t).IsTrue(info.Mode() == 0600, "should still be 0600 mode")
}
