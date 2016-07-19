package osversion

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestVersion(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "os-release")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	versionString := "CentOS release 8.8 (Final) "
	_, err = tmpFile.Write([]byte(versionString))
	if err != nil {
		t.Fatalf("Couldn't write version string to release file: %s", err)
	}

	detector := NewDetector(tmpFile.Name())
	os, version, err := detector.Version()
	if err != nil {
		t.Fatal(err)
	}

	if os != "CentOS" {
		t.Errorf("Expected os version to be 'CentOS', was '%s'", os)
	}

	if version != "8.8" {
		t.Errorf("Expected os version to be '8.8', was '%s'", os)
	}

	err = tmpFile.Truncate(0)
	if err != nil {
		t.Fatalf("Unable to truncate release file: %s", err)
	}

	_, err = tmpFile.Seek(0, 0)
	if err != nil {
		t.Fatalf("Unable to seek to the beginning of the file to write another version: %s", err)
	}

	versionString = "CentOS Linux release 9.0.091761 (Core)"
	_, err = tmpFile.Write([]byte(versionString))
	if err != nil {
		t.Fatalf("Couldn't write version string to release file: %s", err)
	}

	detector = NewDetector(tmpFile.Name())
	os, version, err = detector.Version()
	if err != nil {
		t.Fatal(err)
	}

	if os != "CentOS" {
		t.Errorf("Expected os version to be 'CentOS', was '%s'", os)
	}

	if version != "9.0.091761" {
		t.Errorf("Expected os version to be '9.0.091761', was '%s'", os)
	}
}
