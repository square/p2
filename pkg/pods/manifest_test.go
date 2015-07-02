package pods

import (
	"bytes"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func TestPodManifestCanBeRead(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	testPath := filepath.Join(filepath.Dir(filename), "test_manifest.yaml")

	manifest, err := ManifestFromPath(testPath)
	Assert(t).IsNil(err, "Should not have failed to get pod manifest.")
	Assert(t).AreEqual("hello", manifest.ID(), "Id read from manifest didn't have expected value")
	Assert(t).AreEqual(manifest.LaunchableStanzas["app"].Location, "hoisted-hello_def456.tar.gz", "Location read from manifest didn't have expected value")
	Assert(t).AreEqual("hoist", manifest.LaunchableStanzas["app"].LaunchableType, "LaunchableType read from manifest didn't have expected value")
	Assert(t).AreEqual("hello", manifest.LaunchableStanzas["app"].LaunchableId, "LaunchableId read from manifest didn't have expected value")

	Assert(t).AreEqual("staging", manifest.Config["ENVIRONMENT"], "Should have read the ENVIRONMENT from the config stanza")
	hoptoad := manifest.Config["hoptoad"].(map[interface{}]interface{})
	Assert(t).IsTrue(len(hoptoad) == 3, "Should have read the hoptoad value from the config stanza")
}

// Some tests will break if the order of these keys changes (such as
// TestPodManifestCanBeWritten) because it manually creates a struct and does
// not control the order in which manifest.Write() decides to marshal the yaml
func testPod() string {
	return `id: thepod
launchables:
  my-app:
    launchable_type: hoist
    launchable_id: web
    location: https://localhost:4444/foo/bar/baz.tar.gz
config:
  ENVIRONMENT: staging
status_port: 8000
`
}

func TestPodManifestCanBeWritten(t *testing.T) {
	manifest := Manifest{
		Id:                "thepod",
		LaunchableStanzas: make(map[string]LaunchableStanza),
		Config:            make(map[interface{}]interface{}),
	}
	launchable := LaunchableStanza{
		LaunchableType: "hoist",
		LaunchableId:   "web",
		Location:       "https://localhost:4444/foo/bar/baz.tar.gz",
	}
	manifest.LaunchableStanzas["my-app"] = launchable
	manifest.Config["ENVIRONMENT"] = "staging"

	manifest.StatusPort = 8000

	buff := bytes.Buffer{}
	manifest.Write(&buff)

	expected := testPod()
	Assert(t).AreEqual(expected, buff.String(), "Expected the manifest to marshal to the given yaml")
}

func TestPodManifestCanWriteItsConfigStanzaSeparately(t *testing.T) {
	config := testPod()
	manifest, err := ManifestFromBytes(bytes.NewBufferString(config).Bytes())
	Assert(t).IsNil(err, "should not have erred when building manifest")

	buff := bytes.Buffer{}
	err = manifest.WriteConfig(&buff)
	Assert(t).IsNil(err, "should not have erred when writing the config")
	expected := "ENVIRONMENT: staging\n"
	Assert(t).AreEqual(expected, buff.String(), "config should have been written")
}

func TestPodManifestCanReportItsSHA(t *testing.T) {
	config := testPod()
	manifest, err := ManifestFromBytes(bytes.NewBufferString(config).Bytes())
	Assert(t).IsNil(err, "should not have erred when building manifest")
	val, err := manifest.SHA()
	Assert(t).IsNil(err, "should not have erred when getting SHA")
	Assert(t).AreEqual("17acfa1ce4bdd9674524f8faed383bf365d168c81d9d981d63173a33a7fed5a1", val, "SHA mismatched expectations")
}

func TestNilPodManifestHasEmptySHA(t *testing.T) {
	var manifest *Manifest
	content, err := manifest.SHA()
	Assert(t).AreEqual("", content, "the SHA should have been empty")
	Assert(t).IsNotNil(err, "Should have had an error when attempting to read SHA from nil manifest")
}

func TestRunAs(t *testing.T) {
	config := testPod()
	manifest, err := ManifestFromBytes(bytes.NewBufferString(config).Bytes())
	Assert(t).IsNil(err, "should not have erred when building manifest")

	Assert(t).AreEqual(manifest.RunAsUser(), manifest.ID(), "RunAsUser() didn't match expectations")

	config += `run_as: specialuser`
	manifest, err = ManifestFromBytes(bytes.NewBufferString(config).Bytes())
	Assert(t).IsNil(err, "should not have erred when building manifest")
	Assert(t).AreEqual(manifest.RunAsUser(), "specialuser", "RunAsUser() didn't match expectations")
}

func TestByteOrderPreserved(t *testing.T) {
	// The yaml keys here are intentionally ordered in a way that without special
	// care, the bytes returned by manifest.Bytes() would be in a different order
	// than the bytes passed in to ManifestFromBytes()
	manifestBytes := []byte(`id: thepod
launchables:
  my-app:
    launchable_type: hoist
    launchable_id: web
    location: https://localhost:4444/foo/bar/baz.tar.gz
status_port: 8000
config:
  ENVIRONMENT: staging
`)
	manifest, err := ManifestFromBytes(manifestBytes)
	Assert(t).IsNil(err, "should not have erred constructing manifest from bytes")
	outBytes, err := manifest.OriginalBytes()
	Assert(t).IsNil(err, "should not have erred extracting manifest struct to bytes")
	Assert(t).AreEqual(string(outBytes), string(manifestBytes), "Byte order should not have changed when unmarshaling and remarshaling a manifest")
}
