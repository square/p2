package pods

import (
	"path"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func getTestPod() *Pod {
	_, filename, _, _ := runtime.Caller(0)
	testPath := path.Join(path.Dir(filename), "test_manifest.yaml")
	pod, _ := PodFromManifestPath(testPath)
	return pod
}

func getLaunchableStanzasFromTestManifest() map[string]LaunchableStanza {
	return getTestPod().podManifest.LaunchableStanzas
}

func getPodIdFromTestManifest() string {
	return getTestPod().podManifest.Id
}

func TestGetLaunchable(t *testing.T) {
	launchableStanzas := getLaunchableStanzasFromTestManifest()
	podId := getPodIdFromTestManifest()
	Assert(t).AreNotEqual(0, len(launchableStanzas), "Expected there to be at least one launchable stanza in the test manifest")
	for _, stanza := range launchableStanzas {
		launchable, _ := getLaunchable(stanza, podId)
		Assert(t).AreEqual("hello", launchable.id, "LaunchableId did not have expected value")
		Assert(t).AreEqual("http://localhost:8000/foo/bar/baz/hello_abc123.tar.gz", launchable.location, "Launchable location did not have expected value")
	}
}
