package uri

import (
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/square/p2/pkg/util"
)

// Internet Standard: STD 66
var leadingScheme = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9+\-.]*):`)

// A UriFetcher presents simple methods for fetching URIs from
// different schemes.
type Fetcher interface {
	// Opens a data stream to the source URI. If no URI scheme is
	// specified, treats the URI as a path to a local file.
	Open(uri string) (io.ReadCloser, error)

	// Copy all data from the source URI to a local file at the
	// destination path.
	CopyLocal(srcUri, dstPath string) error
}

// A default fetcher, if the user doesn't want to set any options.
var DefaultFetcher Fetcher = BasicFetcher{http.DefaultClient}

// URICopy Wraps opening and copying content from URIs. Will attempt
// directly perform file copies if the uri is begins with file://, otherwise
// delegates to a curl implementation.
var URICopy = DefaultFetcher.CopyLocal

// BasicFetcher can access "file" and "http" schemes using the OS and
// a provided HTTP client, respectively.
type BasicFetcher struct {
	Client *http.Client
}

func (f BasicFetcher) Open(srcUri string) (io.ReadCloser, error) {
	var scheme, opaque string
	if matches := leadingScheme.FindStringSubmatch(srcUri); matches != nil {
		scheme = strings.ToLower(matches[1])
		opaque = srcUri[len(matches[0]):]
	}
	switch scheme {
	case "":
		// Assume a schemeless URI is a path to a local file
		return os.Open(srcUri)
	case "file":
		// Only allow local, absolute file references
		if !strings.HasPrefix(opaque, "///") {
			return nil, util.Errorf("%s: invalid file URI", srcUri)
		}
		return os.Open(opaque[3:])
	case "http", "https":
		resp, err := f.Client.Get(srcUri)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, util.Errorf(
				"%s: HTTP server returned status: %s",
				srcUri,
				resp.Status,
			)
		}
		return resp.Body, nil
	default:
		return nil, util.Errorf("%s: unhandled URI scheme", srcUri)
	}
}

func (f BasicFetcher) CopyLocal(srcUri, dstPath string) (err error) {
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
	SrcUri  string
	DstPath string
}

func NewLoggedFetcher(fetcher Fetcher) *LoggedFetcher {
	if fetcher == nil {
		fetcher = DefaultFetcher
	}
	return &LoggedFetcher{fetcher, "", ""}
}

func (f *LoggedFetcher) Open(srcUri string) (io.ReadCloser, error) {
	f.SrcUri = srcUri
	f.DstPath = ""
	return f.fetcher.Open(srcUri)
}

func (f *LoggedFetcher) CopyLocal(srcUri, dstPath string) error {
	f.SrcUri = srcUri
	f.DstPath = dstPath
	return f.fetcher.CopyLocal(srcUri, dstPath)
}
