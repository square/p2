package uri

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/util"
)

func TestURIWillCopyFilesCorrectly(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "cp-dest")
	Assert(t).IsNil(err, "Couldn't create temp dir")
	defer os.RemoveAll(tempdir)
	thisFile := util.From(runtime.Caller(0)).Filename
	copied := filepath.Join(tempdir, "copied")
	err = URICopy(fmt.Sprintf("file:///%s", thisFile), copied)
	Assert(t).IsNil(err, "The file should have been copied")
	copiedContents, err := ioutil.ReadFile(copied)
	Assert(t).IsNil(err, "The copied file could not be read")
	thisContents, err := ioutil.ReadFile(thisFile)
	Assert(t).IsNil(err, "The original file could not be read")
	Assert(t).AreEqual(string(thisContents), string(copiedContents), "The contents of the files do not match")
}

func TestURIWithNoProtocolTreatedLikeLocalPath(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "cp-dest")
	Assert(t).IsNil(err, "Couldn't create temp dir")
	defer os.RemoveAll(tempdir)
	thisFile := util.From(runtime.Caller(0)).Filename
	copied := filepath.Join(tempdir, "copied")
	err = URICopy(thisFile, copied)
	Assert(t).IsNil(err, "The file should have been copied")
	copiedContents, err := ioutil.ReadFile(copied)
	thisContents, err := ioutil.ReadFile(thisFile)
	Assert(t).IsNil(err, "The original file could not be read")
	Assert(t).AreEqual(string(thisContents), string(copiedContents), "The contents of the files do not match")
}

func TestCorrectlyPullsFilesOverHTTP(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "cp-dest")
	Assert(t).IsNil(err, "Couldn't create temp dir")
	defer os.RemoveAll(tempdir)

	copied := filepath.Join(tempdir, "copied")

	caller := util.From(runtime.Caller(0))

	ts := httptest.NewServer(http.FileServer(http.Dir(caller.Dirname())))
	defer ts.Close()
	serverURL, err := url.Parse(ts.URL)
	Assert(t).IsNil(err, "should have parsed server URL")

	serverURL.Path = filepath.Base(caller.Filename)
	err = URICopy(serverURL.String(), copied)
	Assert(t).IsNil(err, "the file should have been downloaded")

	copiedContents, err := ioutil.ReadFile(copied)
	thisContents, err := ioutil.ReadFile(caller.Filename)

	Assert(t).AreEqual(string(thisContents), string(copiedContents), "Should have downloaded the file correctly")
}
