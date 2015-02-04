package uri

import (
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/square/p2/pkg/util"
)

var leadingProto = regexp.MustCompile("^[a-zA-Z\\d\\.]+:.*")

type Copier struct {
	Client *http.Client
}

func NewCopier(client *http.Client) *Copier {
	copier := &Copier{
		Client: client,
	}

	return copier
}

var DefaultCopier *Copier = NewCopier(http.DefaultClient)

// URICopy Wraps opening and copying content from URIs. Will attempt
// directly perform file copies if the uri is begins with file://, otherwise
// delegates to a curl implementation.
func URICopy(fromURI, toPath string) error {
	return DefaultCopier.Copy(fromURI, toPath)
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

func (c *Copier) Copy(fromURI, toPath string) error {
	hasProto := leadingProto.MatchString(fromURI)
	if !hasProto {
		return copyFile(toPath, fromURI)
	}
	if strings.HasPrefix(fromURI, "file://") {
		return copyFile(toPath, fromURI[len("file://"):])
	} else {
		resp, err := c.Client.Get(fromURI)
		if err != nil {
			return util.Errorf("Couldn't copy file using HTTP: %s", err)
		}
		defer resp.Body.Close()
		d, err := os.Create(toPath)
		if err != nil {
			return util.Errorf("Couldn't open destination file '%s' for writing: %s", toPath, err)
		}
		_, err = io.Copy(d, resp.Body)
		if err != nil {
			d.Close()
			return util.Errorf("Couldn't copy to the destination file '%s': %s ", toPath, err)
		}
		return d.Close()
	}
}
