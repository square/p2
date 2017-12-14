package artifact

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"testing"

	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/uri"
)

const testLocation = "https://fileserver.com/artifact.tar.gz"

type FakeFetcher struct {
	// Captures the URL it's been asked to fetch most recently.
	FetchedURL *url.URL

	// Data to be returned during fetch calls
	Data []byte
}

func (f *FakeFetcher) Open(uri *url.URL) (io.ReadCloser, error) {
	reader := bytes.NewReader(f.Data)
	readCloser := ioutil.NopCloser(reader)
	f.FetchedURL = uri
	return readCloser, nil
}

func (f *FakeFetcher) Head(url *url.URL) (*http.Response, error) {
	return nil, errors.New("Head not implemented on fake fetcher")
}

func (f *FakeFetcher) CopyLocal(srcUri *url.URL, dstPath string) error {
	return errors.New("CopyLocal not implemented on fake fetcher")
}

var _ uri.Fetcher = &FakeFetcher{}

func fakeFetcherNoData() uri.Fetcher {
	return &FakeFetcher{
		Data: []byte{},
	}
}

func locationLaunchable() launch.LaunchableStanza {
	return launch.LaunchableStanza{
		Location: "https://fileserver.com/artifact.tar.gz",
	}
}

// Returns a registry instance not configured with an artifact registry, so
// it's only useful for tests relying on the "location" method of specifying a
// launchable location
func locationDataRegistry() Registry {
	return NewRegistry(nil, fakeFetcherNoData(), osversion.DefaultDetector)
}

func TestLocationDataForLaunchableWithLocation(t *testing.T) {
	registry := locationDataRegistry()
	location, artifactData, err := registry.LocationDataForLaunchable("pod_id", "launchable_id", locationLaunchable())
	if err != nil {
		t.Fatalf("Unexpected error getting location data: %s", err)
	}

	if location.String() != testLocation {
		t.Errorf(
			"Didn't properly parse artifact location from stanza: wanted '%s' was '%s'",
			testLocation,
			location.String(),
		)
	}

	expectedManifestLocation := testLocation + ".manifest"
	if artifactData.ManifestLocation.String() != expectedManifestLocation {
		t.Errorf(
			"Didn't properly compute manifest location: wanted '%s' was '%s'",
			expectedManifestLocation,
			artifactData.ManifestLocation.String(),
		)
	}

	expectedManifestSignatureLocation := testLocation + ".manifest.sig"
	if artifactData.ManifestSignatureLocation.String() != expectedManifestSignatureLocation {
		t.Errorf(
			"Didn't properly compute manifest signature location: wanted '%s' was '%s'",
			expectedManifestSignatureLocation,
			artifactData.ManifestSignatureLocation.String(),
		)
	}

	expectedBuildSignatureLocation := testLocation + ".sig"
	if artifactData.BuildSignatureLocation.String() != expectedBuildSignatureLocation {
		t.Errorf(
			"Didn't properly compute build signature location: wanted '%s' was '%s'",
			expectedBuildSignatureLocation,
			artifactData.BuildSignatureLocation.String(),
		)
	}
}

func TestNeitherVersionNorLocationInvalid(t *testing.T) {
	launchable := launch.LaunchableStanza{}
	registry := locationDataRegistry()
	_, _, err := registry.LocationDataForLaunchable("pod_id", "launchable_id", launchable)
	if err == nil {
		t.Errorf("Expected an error when launchable has neither version nor location")
	}
}

func TestBothVersionAndLocationInvalid(t *testing.T) {
	launchable := launch.LaunchableStanza{
		Version: launch.LaunchableVersion{
			ID: "some_version",
		},
		Location: testLocation,
	}
	registry := locationDataRegistry()
	_, _, err := registry.LocationDataForLaunchable("pod_id", "launchable_id", launchable)
	if err == nil {
		t.Errorf("Expected an error when launchable has both version and location")
	}
}

func TestVersionScheme(t *testing.T) {
	launchable := launch.LaunchableStanza{
		Version: launch.LaunchableVersion{
			ID:   "some_version",
			Tags: map[string]string{"foo": "bar"},
		},
	}

	// Just set the paths for ease
	artifactPath := "/path/to/artifact"
	manifestPath := "/path/to/manifest"
	manifestSignaturePath := "/path/to/manifest/signature"
	buildSignaturePath := "/path/to/build/signature"

	cannedRegResponse := RegistryResponse{
		ArtifactLocation:          artifactPath,
		ManifestLocation:          manifestPath,
		ManifestSignatureLocation: manifestSignaturePath,
		BuildSignatureLocation:    buildSignaturePath,
	}

	data, err := json.Marshal(cannedRegResponse)
	if err != nil {
		t.Fatalf("Couldn't marshal registry response as JSON: %s", err)
	}

	fakeFetcher := &FakeFetcher{
		Data: data,
	}

	tmpFile, err := ioutil.TempFile("", "os-release")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	osVersion := "10111"
	versionString := fmt.Sprintf("CentOS version %s (foobar)", osVersion)
	_, err = tmpFile.Write([]byte(versionString))
	if err != nil {
		t.Fatalf("Couldn't write version string to release file: %s", err)
	}

	detector := osversion.NewDetector(tmpFile.Name())

	registryHost := "registryhost.com"
	registry := NewRegistry(&url.URL{Scheme: "https", Host: registryHost}, fakeFetcher, detector)
	artifactURL, verificationData, err := registry.LocationDataForLaunchable("pod_id", "launchable_id", launchable)
	if err != nil {
		t.Fatalf("Unexpected error getting location data: %s", err)
	}

	expectedArtifactURL := &url.URL{
		Path: artifactPath,
	}
	if *artifactURL != *expectedArtifactURL {
		t.Errorf("Expected artifact URL to be '%s', was '%s'", expectedArtifactURL.String(), artifactURL.String())
	}

	if verificationData.ManifestLocation == nil {
		t.Fatal("Manifest location unexpectedly nil")
	}

	expectedManifestURL := &url.URL{
		Path: manifestPath,
	}
	if *verificationData.ManifestLocation != *expectedManifestURL {
		t.Errorf("Expected manifest URL to be '%s', was '%s'", expectedManifestURL.String(), verificationData.ManifestLocation.String())
	}

	if verificationData.ManifestSignatureLocation == nil {
		t.Fatal("Manifest signature location unexpectedly nil")
	}

	expectedManifestSignatureURL := &url.URL{
		Path: manifestSignaturePath,
	}
	if *verificationData.ManifestSignatureLocation != *expectedManifestSignatureURL {
		t.Errorf("Expected manifest signature URL to be '%s', was '%s'", expectedManifestSignatureURL.String(), verificationData.ManifestSignatureLocation.String())
	}

	if verificationData.BuildSignatureLocation == nil {
		t.Fatal("Build signature location unexpectedly nil")
	}

	expectedBuildSignatureURL := &url.URL{
		Path: buildSignaturePath,
	}
	if *verificationData.BuildSignatureLocation != *expectedBuildSignatureURL {
		t.Errorf("Expected build signature URL to be '%s', was '%s'", expectedBuildSignatureURL.String(), verificationData.BuildSignatureLocation.String())
	}

	// Now make sure the correct URL was requested
	if fakeFetcher.FetchedURL.Host != registryHost {
		t.Errorf("Expected registry to make request to host '%s', but made request to '%s'", registryHost, fakeFetcher.FetchedURL.Host)
	}

	if fakeFetcher.FetchedURL.Scheme != "https" {
		t.Errorf("Expected registry to make request with scheme 'https', but made request with '%s'", fakeFetcher.FetchedURL.Scheme)
	}

	query := fakeFetcher.FetchedURL.Query()
	// Make sure our version tag was passed
	if query.Get("foo") != "bar" {
		t.Error("Version tag wasn't properly passed, wanted foo=bar included in request URL")
	}

	if query.Get("os") != "CentOS" {
		t.Error("OS version tag wasn't properly passed, wanted os=CentOS included in request URL")
	}

	if query.Get("os_version") != osVersion {
		t.Errorf("OS version tag wasn't properly passed, wanted os_version=%s included in request URL", osVersion)
	}

	if query.Get("version") != launchable.Version.ID.String() {
		t.Errorf("Version tag wasn't properly passed, wanted version=%s included in the request URL", launchable.Version.ID)
	}
}

type fixedDetector struct{}

func (f *fixedDetector) Version() (osversion.OS, osversion.OSVersion, error) {
	return "a", "b", nil
}

func TestDiscoveryArtifactOverride(t *testing.T) {
	fakeFetcher := &FakeFetcher{Data: []byte{}}
	registryHost := "registryhost.com"
	registry := NewRegistry(&url.URL{Scheme: "https", Host: registryHost}, fakeFetcher, &fixedDetector{})

	testCases := []struct {
		ver      launch.LaunchableVersion
		expected launch.ArtifactName
	}{
		{
			ver:      launch.LaunchableVersion{ID: "ver"},
			expected: "launchable_id",
		}, {
			ver: launch.LaunchableVersion{
				ID:               "ver",
				ArtifactOverride: "overridden",
			},
			expected: "overridden",
		},
	}

	for i, testCase := range testCases {
		// Don't care about any return value, even the error.
		// Just care about what URL was hit.
		_, _, _ = registry.LocationDataForLaunchable("pod_id", "launchable_id", launch.LaunchableStanza{Version: testCase.ver})
		expectedPath := discoverBasePath + "/pod_id"
		if fakeFetcher.FetchedURL.Path != expectedPath {
			t.Errorf("Case %d fetched %q instead of %q", i, fakeFetcher.FetchedURL.Path, expectedPath)
		}
		query := fakeFetcher.FetchedURL.Query()
		if query.Get("artifact_name") != testCase.expected.String() {
			t.Errorf("Case %d had artifact_name %q instead of %q", i, query.Get("artifact_name"), testCase.expected)
		}
	}
}
