package pods

import (
	"path"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func TestPodManifest(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	testPath := path.Join(path.Dir(filename), "test_manifest.yaml")

	podManifest, err := ManifestFromPath(testPath)
	Assert(t).IsNil(err, "Should not have failed to get pod manifest.")
	Assert(t).AreEqual("hello", podManifest.Id, "Id read from manifest didn't have expected value")
	Assert(t).AreEqual("http://localhost:8000/sjc1/deployable/hello/hello_303ab3cc6f0692acba97126d99ac021a6f4134fe.tar.gz", podManifest.LaunchableStanzas["app"].Location, "Location read from manifest didn't have expected value")
	Assert(t).AreEqual("hoist", podManifest.LaunchableStanzas["app"].LaunchableType, "LaunchableType read from manifest didn't have expected value")
	Assert(t).AreEqual("hello", podManifest.LaunchableStanzas["app"].LaunchableId, "LaunchableId read from manifest didn't have expected value")
}
