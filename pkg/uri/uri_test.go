package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func TestURIWillCopyFilesCorrectly(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "cp-dest")
	Assert(t).IsNil(err, "Couldn't create temp dir")
	defer os.RemoveAll(tempdir)
	thisFile := From(runtime.Caller(0)).Filename
	copied := path.Join(tempdir, "copied")
	err = URICopy(fmt.Sprintf("file:///%s", thisFile), copied)
	Assert(t).IsNil(err, "The file should have been copied")
	copiedContents, err := ioutil.ReadFile(copied)
	Assert(t).IsNil(err, "The copied file could not be read")
	thisContents, err := ioutil.ReadFile(thisFile)
	Assert(t).IsNil(err, "The original file could not be read")
	Assert(t).AreEqual(string(thisContents), string(copiedContents), "The contents of the files do not match")
}
