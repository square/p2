package osversion

import (
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/square/p2/pkg/util"
)

const (
	redhatReleaseFile    = "/etc/redhat-release"
	supportedOSName   OS = "CentOS"
)

var redhatVersionRegex = regexp.MustCompile("^\\d[\\d\\.]*$")

// Represents an operating system name, e.g. "CentOS"
type OS string

func (o OS) String() string { return string(o) }

// Represents an operating system version, e.g. "6.6"
type OSVersion string

func (v OSVersion) String() string { return string(v) }

type Detector interface {
	Version() (OS, OSVersion, error)
}

func NewDetector(releaseFile string) Detector {
	return detector{
		releaseFile: releaseFile,
	}
}

type detector struct {
	// The path to the file from which the release should be read
	releaseFile string
}

var DefaultDetector = NewDetector(redhatReleaseFile)

// Attempts to detect the operating system and its version from the underlyin
// system. Currently this function will return an error unless:
// 1) /etc/redhat-release exists and is readable
// 2) The contents of /etc/redhat-release start with "CentOS " (trailing space
// intended)
// 3) After separating the contents of /etc/redhat-release by spaces, the
// second to last entry in the array matches the regex "^\\d[\\d\\.]*$", that
// is to say it starts with a numerical digit and is followed by any
// combination of periods and digits
func (d detector) Version() (OS, OSVersion, error) {
	fileBytes, err := ioutil.ReadFile(d.releaseFile)
	if err != nil {
		return "", "", util.Errorf("Can't detect OS: %s", err)
	}

	releaseParts := strings.Split(string(fileBytes), " ")

	osName := OS(releaseParts[0])
	if osName != supportedOSName {
		return "", "", util.Errorf("Version detection is only supported for '%s', not '%s'", supportedOSName, osName)
	}

	version := OSVersion(releaseParts[len(releaseParts)-2])
	if !redhatVersionRegex.MatchString(version.String()) {
		return "", "", util.Errorf("OS version '%s' did not match expected regex", version)
	}

	return osName, version, nil
}
