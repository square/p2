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
	Assert(t).AreEqual("hello", manifest.Id, "Id read from manifest didn't have expected value")
	Assert(t).AreEqual("http://localhost:8000/foo/bar/baz/hello_abc123.tar.gz", manifest.LaunchableStanzas["app"].Location, "Location read from manifest didn't have expected value")
	Assert(t).AreEqual("hoist", manifest.LaunchableStanzas["app"].LaunchableType, "LaunchableType read from manifest didn't have expected value")
	Assert(t).AreEqual("hello", manifest.LaunchableStanzas["app"].LaunchableId, "LaunchableId read from manifest didn't have expected value")

	Assert(t).AreEqual("staging", manifest.Config["ENVIRONMENT"], "Should have read the ENVIRONMENT from the config stanza")
	hoptoad := manifest.Config["hoptoad"].(map[interface{}]interface{})
	Assert(t).IsTrue(len(hoptoad) == 3, "Should have read the hoptoad value from the config stanza")
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

	buff := bytes.Buffer{}
	manifest.Write(&buff)

	expected := `id: thepod
launchables:
  my-app:
    launchable_type: hoist
    launchable_id: web
    location: https://localhost:4444/foo/bar/baz.tar.gz
config:
  ENVIRONMENT: staging
`
	Assert(t).AreEqual(expected, buff.String(), "Expected the manifest to marshal to the given yaml")
}
