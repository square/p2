package artifact

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"

	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

const (
	discoverBasePath = "/discover"
	artifactNameTag  = "artifact_name"
	osTag            = "os"
	osVersionTag     = "os_version"
	versionTag       = "version"
)

// interface for running operations against an artifact registry.
type Registry interface {
	// Given a LaunchableStanza from a pod manifest, returns a URL from which the
	// artifact can be fetched an a struct containing the locations of files that
	// can be used to verify artifact integrity
	LocationDataForLaunchable(podID store.PodID, launchableID launch.LaunchableID, stanza launch.LaunchableStanza) (*url.URL, auth.VerificationData, error)
}

type registry struct {
	registryURL       *url.URL
	fetcher           uri.Fetcher
	osVersionDetector osversion.Detector
}

func NewRegistry(registryURL *url.URL, fetcher uri.Fetcher, osVersionDetector osversion.Detector) Registry {
	if osVersionDetector == nil {
		osVersionDetector = osversion.DefaultDetector
	}

	return &registry{
		registryURL:       registryURL,
		fetcher:           fetcher,
		osVersionDetector: osVersionDetector,
	}
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
func (a registry) LocationDataForLaunchable(podID store.PodID, launchableID launch.LaunchableID, stanza launch.LaunchableStanza) (*url.URL, auth.VerificationData, error) {
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

	if a.registryURL == nil {
		return nil, auth.VerificationData{}, util.Errorf("No artifact registry configured and location field not present on launchable %s", launchableID)
	}

	return a.fetchRegistryData(podID, launchableID, stanza.Version)
}

type RegistryResponse struct {
	ArtifactLocation          string `json:"location"`
	ManifestLocation          string `json:"manifest_location"`
	ManifestSignatureLocation string `json:"manifest_signature_location"`
	BuildSignatureLocation    string `json:"signature_location"`
}

func (a registry) fetchRegistryData(podID store.PodID, launchableID launch.LaunchableID, version launch.LaunchableVersion) (*url.URL, auth.VerificationData, error) {
	requestURL := &url.URL{
		Path: fmt.Sprintf("%s/%s", discoverBasePath, podID),
	}
	query := url.Values{}
	for key, val := range version.Tags {
		query.Add(key, val)
	}

	os, osVersion, err := a.osVersionDetector.Version()
	if err != nil {
		return nil, auth.VerificationData{}, err
	}

	if version.ArtifactOverride.String() != "" {
		query.Add(artifactNameTag, version.ArtifactOverride.String())
	} else {
		query.Add(artifactNameTag, launchableID.String())
	}
	query.Add(osTag, os.String())
	query.Add(osVersionTag, osVersion.String())
	query.Add(versionTag, version.ID.String())

	requestURL.RawQuery = query.Encode()

	data, err := a.fetcher.Open(a.registryURL.ResolveReference(requestURL))
	if err != nil {
		return nil, auth.VerificationData{}, err
	}
	defer data.Close()

	respBytes, err := ioutil.ReadAll(data)
	if err != nil {
		return nil, auth.VerificationData{}, util.Errorf("Could not read response from artifact registry: %s", err)
	}

	var registryResponse RegistryResponse
	err = json.Unmarshal(respBytes, &registryResponse)
	if err != nil {
		l := len(respBytes)
		if l > 80 {
			l = 80
		}
		return nil, auth.VerificationData{}, util.Errorf(
			"bad response from artifact registry: %s: %q",
			err,
			string(respBytes[:l]),
		)
	}

	// Require artifact URL to be present, other fields are optional but returned
	var artifactURL *url.URL
	if registryResponse.ArtifactLocation == "" {
		return nil, auth.VerificationData{}, util.Errorf("No artifact url returned in registry response")
	} else {
		artifactURL, err = url.Parse(registryResponse.ArtifactLocation)
		if err != nil {
			return nil, auth.VerificationData{}, util.Errorf("Could not parse artifact url in registry response: %s", err)
		}
	}

	authData, err := a.authDataFromRegistryResponse(registryResponse)
	if err != nil {
		return nil, auth.VerificationData{}, err
	}

	return artifactURL, authData, nil
}

func (a registry) authDataFromRegistryResponse(registryResponse RegistryResponse) (auth.VerificationData, error) {
	verificationData := auth.VerificationData{}
	if registryResponse.ManifestLocation != "" {
		manifestURL, err := url.Parse(registryResponse.ManifestLocation)
		if err != nil {
			return verificationData, util.Errorf("Couldn't parse manifest URL from registry response: %s", err)
		}
		verificationData.ManifestLocation = manifestURL
	}

	if registryResponse.ManifestSignatureLocation != "" {
		manifestSignatureURL, err := url.Parse(registryResponse.ManifestSignatureLocation)
		if err != nil {
			return verificationData, util.Errorf("Couldn't parse manifest signature URL from registry response: %s", err)
		}
		verificationData.ManifestSignatureLocation = manifestSignatureURL
	}

	if registryResponse.BuildSignatureLocation != "" {
		buildSignatureURL, err := url.Parse(registryResponse.BuildSignatureLocation)
		if err != nil {
			return verificationData, util.Errorf("Couldn't parse build signature URL from registry response: %s", err)
		}
		verificationData.BuildSignatureLocation = buildSignatureURL
	}

	return verificationData, nil
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
