package artifact

import (
	"testing"

	"github.com/square/p2/pkg/types"
)

const testLocation = "https://fileserver.com/artifact.tar.gz"

func locationLaunchable() types.LaunchableStanza {
	return types.LaunchableStanza{
		Location: "https://fileserver.com/artifact.tar.gz",
	}
}

func TestLocationDataForLaunchableWithLocation(t *testing.T) {
	registry := NewRegistry()
	location, artifactData, err := registry.LocationDataForLaunchable(locationLaunchable())
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
	launchable := types.LaunchableStanza{}
	registry := NewRegistry()
	_, _, err := registry.LocationDataForLaunchable(launchable)
	if err == nil {
		t.Errorf("Expected an error when launchable has neither version nor location")
	}
}

func TestBothVersionAndLocationInvalid(t *testing.T) {
	launchable := types.LaunchableStanza{
		Version: types.LaunchableVersion{
			ID: "some_version",
		},
		Location: testLocation,
	}
	registry := NewRegistry()
	_, _, err := registry.LocationDataForLaunchable(launchable)
	if err == nil {
		t.Errorf("Expected an error when launchable has both version and location")
	}
}

func TestVersionSchemeNotImplemented(t *testing.T) {
	launchable := types.LaunchableStanza{
		Version: types.LaunchableVersion{
			ID: "some_version",
		},
	}
	registry := NewRegistry()
	_, _, err := registry.LocationDataForLaunchable(launchable)
	if err == nil {
		t.Errorf("Expected a not implemented error when launchable version only")
	}
}
