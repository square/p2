package launch

import (
	"testing"
)

func TestVersionFromLocation(t *testing.T) {
	type testExpectation struct {
		Location        string
		ExpectedVersion string
		ExpectError     bool
	}

	expectations := []testExpectation{
		{
			Location:        "/download/test-launchable_3c021aff048ca8117593f9c71e03b87cf72fd440.tar.gz",
			ExpectedVersion: "3c021aff048ca8117593f9c71e03b87cf72fd440",
			ExpectError:     false,
		},
		{
			Location:        "/download/test04_launchable2_3c021aff048ca8117593f9c71e03b87cf72fd440.tar.gz",
			ExpectedVersion: "3c021aff048ca8117593f9c71e03b87cf72fd440",
			ExpectError:     false,
		},
		{
			Location:        "/download/test-launchable_3c021aff048ca8117593f9c71e03b87cf72fd440-suffix.tar.gz",
			ExpectedVersion: "3c021aff048ca8117593f9c71e03b87cf72fd440-suffix",
			ExpectError:     false,
		},
		{
			Location:        "/download/afb1.2.00.tar.gz",
			ExpectedVersion: "",
			ExpectError:     true,
		},
		{
			Location:        "/download/jdk-2.9.0_22.tar.gz",
			ExpectedVersion: "",
			ExpectError:     true,
		},
	}

	for _, expectation := range expectations {
		version, err := versionFromLocation(expectation.Location)
		if expectation.ExpectError && err == nil {
			t.Errorf("Expected an error parsing version from '%s', but there wasn't one", expectation.Location)
		}

		if !expectation.ExpectError && err != nil {
			t.Errorf("Unexpected error occurred parsing version from '%s': %s", expectation.Location, err)
		}

		if version != expectation.ExpectedVersion {
			t.Errorf("Expected version for '%s' to be '%s', was '%s'", expectation.Location, expectation.ExpectedVersion, version)
		}
	}
}
