package uri

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/util"
)

func TestLeadingProtoRegexMatchesProto(t *testing.T) {
	Assert(t).IsTrue(leadingProto.MatchString("file:///foo/bar/baz"), "Should have matched")
	Assert(t).IsTrue(leadingProto.MatchString("http://www.com/foo/bar/baz"), "Should have matched")
}

func TestLeadingProtoRegexDoesNotMatchPath(t *testing.T) {
	Assert(t).IsFalse(leadingProto.MatchString("/foo/bar/baz"), "Should not have matched")
}

func TestURIWillCopyFilesCorrectly(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "cp-dest")
	Assert(t).IsNil(err, "Couldn't create temp dir")
	defer os.RemoveAll(tempdir)
	thisFile := util.From(runtime.Caller(0)).Filename
	copied := path.Join(tempdir, "copied")
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
	copied := path.Join(tempdir, "copied")
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

	copied := path.Join(tempdir, "copied")

	caller := util.From(runtime.Caller(0))

	ts := httptest.NewServer(http.FileServer(http.Dir(caller.Dirname())))
	Assert(t).IsTrue(leadingProto.MatchString(ts.URL), fmt.Sprintf("the http test server generated an invalid url (%s)", ts.URL))
	defer ts.Close()

	// Do not use path.Join for URLs. It will compress consecutive forward slashes.
	// ie, http:// becomes http:/
	source := fmt.Sprintf("%s/%s", ts.URL, path.Base(caller.Filename))
	Assert(t).IsTrue(leadingProto.MatchString(source), fmt.Sprintf("The url %s should have had a proto", source))

	err = URICopy(source, copied)
	Assert(t).IsNil(err, "the file should have been downloaded")

	copiedContents, err := ioutil.ReadFile(copied)
	thisContents, err := ioutil.ReadFile(caller.Filename)

	Assert(t).AreEqual(string(thisContents), string(copiedContents), fmt.Sprintf("Should have downloaded the file correctly from (%s)", source))
}
