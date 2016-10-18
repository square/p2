package uri

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	storage "google.golang.org/api/storage/v1"
)

func init() {
	client, err := google.DefaultClient(context.Background(), storage.DevstorageReadOnlyScope)
	if err != nil {
		logging.DefaultLogger.WithError(err).Debug("Unable to setup Google client, skipping")
		return
	}
	service, err := storage.New(client)
	if err != nil {
		panic(util.Errorf("Unable to setup Google Storage client: %s", err))
	}
	DefaultFetcher.GoogleStorage = service
}

// A Fetcher presents simple methods for fetching URIs from
// different schemes.
type Fetcher interface {
	// Opens a data stream to the source URI. If no URI scheme is
	// specified, treats the URI as a path to a local file.
	Open(uri *url.URL) (io.ReadCloser, error)

	// Copy all data from the source URI to a local file at the
	// destination path.
	CopyLocal(srcUri *url.URL, dstPath string) error
}

// A default fetcher, if the user doesn't want to set any options.
var DefaultFetcher = BasicFetcher{Client: http.DefaultClient}

// URICopy Wraps opening and copying content from URIs. Will attempt
// directly perform file copies if the uri is begins with file://, otherwise
// delegates to a curl implementation.
var URICopy = DefaultFetcher.CopyLocal

// BasicFetcher can access the "file" scheme using the OS, "gs" scheme using the
// Google SDK, and the "http" scheme using the provided HTTP client.
type BasicFetcher struct {
	Client        *http.Client
	GoogleStorage *storage.Service
}

func (f BasicFetcher) Open(u *url.URL) (io.ReadCloser, error) {
	switch u.Scheme {
	case "":
		// Assume a schemeless URI is a path to a local file
		return os.Open(u.String())
	case "file":
		if u.Path == "" {
			return nil, util.Errorf("%s: invalid path in URI", u.String())
		}
		if !filepath.IsAbs(u.Path) {
			return nil, util.Errorf("%q: file URIs must use an absolute path", u.Path)
		}

		return os.Open(u.Path)
	case "gs":
		// Chop off the leading slash
		resp, err := f.GoogleStorage.Objects.Get(u.Host, u.Path[1:]).Download()
		if err != nil {
			return nil, util.Errorf("%q: Unable to download: %s", u.String(), err)
		}
		return resp.Body, nil
	case "http", "https":
		resp, err := f.Client.Get(u.String())
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return nil, util.Errorf(
				"%q: HTTP server returned status: %s",
				u.String(),
				resp.Status,
			)
		}
		return resp.Body, nil
	default:
		return nil, util.Errorf("%q: unknown scheme %s", u.String(), u.Scheme)
	}
}

func (f BasicFetcher) CopyLocal(srcUri *url.URL, dstPath string) (err error) {
	src, err := f.Open(srcUri)
	if err != nil {
		return
	}
	defer src.Close()
	dest, err := os.Create(dstPath)
	if err != nil {
		return
	}
	defer func() {
		// Return the Close() error unless another error happened first
		if errC := dest.Close(); err == nil {
			err = errC
		}
	}()
	_, err = io.Copy(dest, src)
	return
}

// A LoggedFetcher wraps another uri.Fetcher, forwarding all calls and
// recording their arguments. Useful for unit testing.
type LoggedFetcher struct {
	fetcher Fetcher
	SrcUri  *url.URL
	DstPath string
}

func NewLoggedFetcher(fetcher Fetcher) *LoggedFetcher {
	if fetcher == nil {
		fetcher = DefaultFetcher
	}
	return &LoggedFetcher{fetcher, &url.URL{}, ""}
}

func (f *LoggedFetcher) Open(srcUri *url.URL) (io.ReadCloser, error) {
	f.SrcUri = srcUri
	f.DstPath = ""
	return f.fetcher.Open(srcUri)
}

func (f *LoggedFetcher) CopyLocal(srcUri *url.URL, dstPath string) error {
	f.SrcUri = srcUri
	f.DstPath = dstPath
	return f.fetcher.CopyLocal(srcUri, dstPath)
}
