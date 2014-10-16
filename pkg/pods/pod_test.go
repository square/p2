package pods

import (
	"path"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func getTestManifest() *PodManifest {
	_, filename, _, _ := runtime.Caller(0)
	testPath := path.Join(path.Dir(filename), "test_manifest.yaml")
	pod, _ := PodFromManifestPath(testPath)
	return pod.podManifest
}

func getLaunchableStanzasFromTestManifest() map[string]LaunchableStanza {
	return getTestManifest().LaunchableStanzas
}

func getPodIdFromTestManifest() string {
	return getTestManifest().Id
}

func TestGetLaunchable(t *testing.T) {
	launchableStanzas := getLaunchableStanzasFromTestManifest()
	podId := getPodIdFromTestManifest()
	Assert(t).AreNotEqual(0, len(launchableStanzas), "Expected there to be at least one launchable stanza in the test manifest")
	for _, stanza := range launchableStanzas {
		launchable, _ := getLaunchable(stanza, podId)
		Assert(t).AreEqual("hello", launchable.id, "LaunchableId did not have expected value")
		Assert(t).AreEqual("http://localhost:8000/sjc1/deployable/hello/hello_303ab3cc6f0692acba97126d99ac021a6f4134fe.tar.gz", launchable.location, "Launchable location did not have expected value")
	}
}
