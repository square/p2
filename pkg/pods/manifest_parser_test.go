package pods

import (
	"bytes"
	"path"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func TestPodManifestCanBeRead(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	testPath := path.Join(path.Dir(filename), "test_manifest.yaml")

	manifest, err := PodManifestFromPath(testPath)
	Assert(t).IsNil(err, "Should not have failed to get pod manifest.")
	Assert(t).AreEqual("hello", manifest.ID(), "Id read from manifest didn't have expected value")
	Assert(t).AreEqual(manifest.LaunchableStanzas["app"].Location, "hoisted-hello_def456.tar.gz", "Location read from manifest didn't have expected value")
	Assert(t).AreEqual("hoist", manifest.LaunchableStanzas["app"].LaunchableType, "LaunchableType read from manifest didn't have expected value")
	Assert(t).AreEqual("hello", manifest.LaunchableStanzas["app"].LaunchableId, "LaunchableId read from manifest didn't have expected value")

	Assert(t).AreEqual("staging", manifest.Config["ENVIRONMENT"], "Should have read the ENVIRONMENT from the config stanza")
	hoptoad := manifest.Config["hoptoad"].(map[interface{}]interface{})
	Assert(t).IsTrue(len(hoptoad) == 3, "Should have read the hoptoad value from the config stanza")
}

func testPod() string {
	return `id: thepod
launchables:
  my-app:
    launchable_type: hoist
    launchable_id: web
    location: https://localhost:4444/foo/bar/baz.tar.gz
config:
  ENVIRONMENT: staging
port: 8000
`
}

func TestPodManifestCanBeWritten(t *testing.T) {
	manifest := PodManifest{
		Id:                "thepod",
		LaunchableStanzas: make(map[string]LaunchableStanza),
		Config:            make(map[string]interface{}),
	}
	launchable := LaunchableStanza{
		LaunchableType: "hoist",
		LaunchableId:   "web",
		Location:       "https://localhost:4444/foo/bar/baz.tar.gz",
	}
	manifest.LaunchableStanzas["my-app"] = launchable
	manifest.Config["ENVIRONMENT"] = "staging"

	manifest.Port = 8000

	buff := bytes.Buffer{}
	manifest.Write(&buff)

	expected := testPod()
	Assert(t).AreEqual(expected, buff.String(), "Expected the manifest to marshal to the given yaml")
}

func TestPodManifestCanWriteItsConfigStanzaSeparately(t *testing.T) {
	config := testPod()
	manifest, err := PodManifestFromBytes(bytes.NewBufferString(config).Bytes())
	Assert(t).IsNil(err, "should not have erred when building manifest")

	buff := bytes.Buffer{}
	err = manifest.WriteConfig(&buff)
	Assert(t).IsNil(err, "should not have erred when writing the config")
	expected := "ENVIRONMENT: staging\n"
	Assert(t).AreEqual(expected, buff.String(), "config should have been written")
}

func TestPodManifestCanReportItsSHA(t *testing.T) {
	config := testPod()
	manifest, err := PodManifestFromBytes(bytes.NewBufferString(config).Bytes())
	Assert(t).IsNil(err, "should not have erred when building manifest")
	val, err := manifest.SHA()
	Assert(t).IsNil(err, "should not have erred when getting SHA")
	Assert(t).AreEqual("20c18072f4170e5feec92b9f83a1774f2e53ac52", val, "SHA mismatched expectations")
}

func TestNilPodManifestHasEmptySHA(t *testing.T) {
	var manifest *PodManifest
	content, err := manifest.SHA()
	Assert(t).AreEqual("", content, "the SHA should have been empty")
	Assert(t).IsNotNil(err, "Should have had an error when attempting to read SHA from nil manifest")
}
