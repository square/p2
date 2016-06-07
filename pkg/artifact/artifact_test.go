package artifact

import (
	"path"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func getTestArtifact(t *testing.T) *Artifact {
	_, filename, _, _ := runtime.Caller(1)
	artifactPath := path.Join(path.Dir(filename), "myapp_123.tar.gz")
	art, err := NewArtifact(artifactPath)
	Assert(t).IsNil(err, "the test artifact wasn't present")
	return art
}

func TestArtifactsCanDeriveApplications(t *testing.T) {
	art := getTestArtifact(t)
	Assert(t).AreEqual("myapp", art.App().Name, "the app should have been myapp")
}

func TestArtifactsCanReadAppManifests(t *testing.T) {
	art := getTestArtifact(t)
	manifest, err := art.AppManifest()
	Assert(t).IsNil(err, "should not have failed to get the app manifest")
	Assert(t).IsTrue(len(manifest.Ports) > 0, "Should have had more than one port")
	Assert(t).AreEqual(manifest.Ports[43770][0], "http", "Should have retrieved port from app manifest")
}

func TestArtifactNaming(t *testing.T) {
	Assert(t).IsTrue(
		tarAppNameParse.MatchString("myapp_123.tar.gz"),
		"myapp_123.tar.gz",
	)
	Assert(t).IsTrue(
		tarAppNameParse.MatchString("myapp_123.tar"),
		"myapp_123.tar",
	)
	Assert(t).IsTrue(
		tarAppNameParse.MatchString("ab_cd_efg.tar"),
		"ab_cd_efg.tar",
	)
	Assert(t).IsFalse(
		tarAppNameParse.MatchString("myapp_123.tar.bz2"),
		"myapp_123.tar.bz2",
	)
	Assert(t).IsFalse(
		tarAppNameParse.MatchString("mÿapp_123.tar.gz"),
		"mÿapp_123.tar.gz",
	)
}
