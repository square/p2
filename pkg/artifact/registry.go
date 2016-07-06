package artifact

import (
	"net/url"

	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/util"
)

// interface for running operations against an artifact registry.
type Registry interface {
	// Given a LaunchableStanza from a pod manifest, returns a URL from which the
	// artifact can be fetched an a struct containing the locations of files that
	// can be used to verify artifact integrity
	LocationDataForLaunchable(stanza manifest.LaunchableStanza) (*url.URL, auth.VerificationData, error)
}

type registry struct{}

func NewRegistry() Registry {
	return &registry{}
}

// Given a launchable stanza, returns the URL from which the artifact may be downloaded, as
// well as an auth.VerificationData which can be used to verify the artifact.
// There are two schemes for specifying this information in a launchable stanza:
// 1) using the "location" field. In this case, the artifact location is simply the value
// of the field and the path to the verification files is inferred using magic suffixes
// 2) the "version" field is provided. In this case, the artifact registry is queried with
// the information specified under the "version" key and the response contains the URLs
// from which the extra files may be fetched, and these are returned.
//
// When using the first method, the following magical suffixes are assumed:
// manifest: ".manifest"
// manifest signature: ".manifest.sig"
// build signature: ".sig"
func (a registry) LocationDataForLaunchable(stanza manifest.LaunchableStanza) (*url.URL, auth.VerificationData, error) {
	if stanza.Location == "" && stanza.Version.ID == "" {
		return nil, auth.VerificationData{}, util.Errorf("Launchable must provide either \"location\" or \"version\" fields")
	}

	if stanza.Location != "" && stanza.Version.ID != "" {
		return nil, auth.VerificationData{}, util.Errorf("Launchable must not provide both \"location\" and \"version\" fields")
	}

	// infer the verification data using magical suffixes
	if stanza.Location != "" {
		location, err := url.Parse(stanza.Location)
		if err != nil {
			return nil, auth.VerificationData{}, util.Errorf("Couldn't parse launchable url '%s': %s", stanza.Location, err)
		}

		verificationData := VerificationDataForLocation(location)
		return location, verificationData, nil
	}

	return nil, auth.VerificationData{}, util.Errorf("Version key in launchable stanza not implemented")
}

func VerificationDataForLocation(location *url.URL) auth.VerificationData {
	manifestLocation := &url.URL{}
	*manifestLocation = *location
	manifestLocation.Path = location.Path + ".manifest"

	manifestSignatureLocation := &url.URL{}
	*manifestSignatureLocation = *manifestLocation
	manifestSignatureLocation.Path = manifestLocation.Path + ".sig"

	buildSignatureLocation := &url.URL{}
	*buildSignatureLocation = *location
	buildSignatureLocation.Path = location.Path + ".sig"
	return auth.VerificationData{
		ManifestLocation:          manifestLocation,
		ManifestSignatureLocation: manifestSignatureLocation,
		BuildSignatureLocation:    buildSignatureLocation,
	}
}
