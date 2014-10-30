package util

import (
	"io"
	"os"
	"strings"

	"github.com/nareix/curl"
)

// URICopy Wraps opening and copying content from URIs. Will attempt
// directly perform file copies if the uri is begins with file://, otherwise
// delegates to a curl implementation.
func URICopy(uri, path string, opts ...interface{}) error {
	if strings.HasPrefix(uri, "file://") {
		return CopyFile(path, uri[len("file://"):])
	} else {
		return curl.File(uri, path, opts...)
	}
}

func CopyFile(dst, src string) error {
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	defer s.Close()
	d, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(d, s); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
