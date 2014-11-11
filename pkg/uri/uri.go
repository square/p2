package uri

import (
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/nareix/curl"
	"github.com/square/p2/pkg/util"
)

var leadingProto = regexp.MustCompile("^[a-zA-Z\\d\\.]+:.//.*")

// URICopy Wraps opening and copying content from URIs. Will attempt
// directly perform file copies if the uri is begins with file://, otherwise
// delegates to a curl implementation.
func URICopy(uri, path string, opts ...interface{}) error {
	hasProto := leadingProto.MatchString(uri)
	if !hasProto {
		return copyFile(path, uri)
	}
	if strings.HasPrefix(uri, "file://") {
		return copyFile(path, uri[len("file://"):])
	} else {
		return curl.File(uri, path, opts...)
	}
}

func copyFile(dst, src string) error {
	s, err := os.Open(src)
	if err != nil {
		return util.Errorf("Couldn't open source file '%s' for reading: %s", src, err)
	}
	defer s.Close()
	d, err := os.Create(dst)
	if err != nil {
		return util.Errorf("Couldn't open destination file '%s' for writing: %s", dst, err)
	}
	if _, err := io.Copy(d, s); err != nil {
		d.Close()
		return util.Errorf("Couldn't copy file contents to the destination: %s", err)
	}
	return d.Close()
}
