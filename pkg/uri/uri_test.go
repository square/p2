package uri

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/util"
	storage "google.golang.org/api/storage/v1"
)

func TestURIWillCopyFilesCorrectly(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "cp-dest")
	Assert(t).IsNil(err, "Couldn't create temp dir")
	defer os.RemoveAll(tempdir)

	thisFile := util.From(runtime.Caller(0)).Filename
	url := &url.URL{
		Path: thisFile,
	}

	copied := filepath.Join(tempdir, "copied")
	err = URICopy(url, copied)
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
	url := &url.URL{
		Path: thisFile,
	}

	copied := filepath.Join(tempdir, "copied")
	err = URICopy(url, copied)
	Assert(t).IsNil(err, "The file should have been copied")

	copiedContents, err := ioutil.ReadFile(copied)
	Assert(t).IsNil(err, "Couldn't read file")

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
	err = URICopy(serverURL, copied)
	Assert(t).IsNil(err, "the file should have been downloaded")

	copiedContents, err := ioutil.ReadFile(copied)
	thisContents, err := ioutil.ReadFile(caller.Filename)

	Assert(t).AreEqual(string(thisContents), string(copiedContents), "Should have downloaded the file correctly")
}

type fakeRoundTripper struct {
	callback func(req *http.Request) (*http.Response, error)
}

func (frt *fakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if frt.callback != nil {
		return frt.callback(req)
	}
	panic("fakeRoundTripper not setup")
}

func TestCorrectlyPullsFilesFromGoogleStorage(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "cp-dest")
	Assert(t).IsNil(err, "Couldn't create temp dir")
	defer os.RemoveAll(tempdir)

	caller := util.From(runtime.Caller(0))

	copied := filepath.Join(tempdir, "copied")
	thisContents, err := ioutil.ReadFile(caller.Filename)
	Assert(t).IsNil(err, "Couldn't create file")

	// Setup our proxying http client
	frt := &fakeRoundTripper{
		callback: func(req *http.Request) (*http.Response, error) {
			Assert(t).AreEqual("GET", req.Method, "expected a GET request")
			Assert(t).AreEqual("/storage/v1/b/example/o/uri_test.go", req.URL.Path, "expected the correct request path")
			Assert(t).AreEqual("www.googleapis.com", req.URL.Host, "expected the call to go to a google host")
			Assert(t).AreEqual(req.URL.Query().Get("alt"), "media", "expected the call to ask to download the file")

			return &http.Response{
				Status:     strconv.Itoa(200),
				StatusCode: 200,
				Body:       ioutil.NopCloser(bytes.NewReader(thisContents)),
				Header:     http.Header{},
			}, nil
		},
	}

	service, err := storage.New(&http.Client{Transport: frt})
	Assert(t).IsNil(err, "should have created the google storage client")
	fetcher := BasicFetcher{GoogleStorage: service}

	parsed, err := url.Parse("gs://example/"+filepath.Base(caller.Filename))
	Assert(t).IsNil(err, "the url should have parsed")

	err = fetcher.CopyLocal(parsed, copied)
	Assert(t).IsNil(err, "the file should have been downloaded")

	copiedContents, err := ioutil.ReadFile(copied)

	Assert(t).AreEqual(string(thisContents), string(copiedContents), "Should have downloaded the file correctly")
}
