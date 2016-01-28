package pods

import (
	"bytes"
	"errors"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/util/size"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/yaml.v2"
)

func TestPodManifestCanBeRead(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	testPath := filepath.Join(filepath.Dir(filename), "test_manifest.yaml")

	manifest, err := ManifestFromPath(testPath)
	Assert(t).IsNil(err, "Should not have failed to get pod manifest.")
	Assert(t).AreEqual("hello", string(manifest.ID()), "Id read from manifest didn't have expected value")
	Assert(t).AreEqual(manifest.GetLaunchableStanzas()["app"].Location, "hoisted-hello_def456.tar.gz", "Location read from manifest didn't have expected value")
	Assert(t).AreEqual("hoist", manifest.GetLaunchableStanzas()["app"].LaunchableType, "LaunchableType read from manifest didn't have expected value")
	Assert(t).AreEqual("app", manifest.GetLaunchableStanzas()["app"].LaunchableId, "LaunchableId read from manifest didn't have expected value")

	Assert(t).AreEqual("staging", manifest.GetConfig()["ENVIRONMENT"], "Should have read the ENVIRONMENT from the config stanza")
	hoptoad := manifest.GetConfig()["hoptoad"].(map[interface{}]interface{})
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
    cgroup:
      cpus: 4
      memory: 1073741824
config:
  ENVIRONMENT: staging
status_port: 8000
`
}

func testSignedPod() string {
	return `
-----BEGIN PGP SIGNED MESSAGE-----
Hash: SHA256

id: thepod
launchables:
  my-app:
    launchable_type: hoist
    launchable_id: web
    location: https://localhost:4444/foo/bar/baz.tar.gz
status_port: 8000
config:
  ENVIRONMENT: staging
-----BEGIN PGP SIGNATURE-----
Version: GnuPG v2

iQEcBAEBCAAGBQJV3LVOAAoJEFvqkbrdatOH63cH/3NiqIBRA+dI7k4KT2CtUQrk
QX4aYfmTvqcf/PqeilJiVWL+fWfP/EAM/cNSsg4rRylmnRyxG2BDL4wn12oWEDe5
jmv/XGEb7vBgOZfOTeFLiorViMMKFYqXFqQZp2jwAdEUgzF4AiLyg9+MX2B19mOV
c6o8YRrhIMGiWIA6rTDX53ruk93p7z/axKGJ0nonxJG3u7Be2MsPlYEavZLtKAg1
0GyjcFe676Kqz8i1RiyelA/+zp8qaS+jWemrKRrDwmMxAY5j5eFxPFk2mxRems6J
QyB184dCJQnFbcQslyXDSR4Lal12NPvxbtK/4YYXZZVwf4hKCfVqvmG2zgwINDc=
=IMbK
-----END PGP SIGNATURE-----`
}

func TestPodManifestCanBeWritten(t *testing.T) {
	builder := NewManifestBuilder()
	builder.SetID("thepod")
	launchables := map[string]LaunchableStanza{
		"my-app": {
			LaunchableType: "hoist",
			LaunchableId:   "web",
			Location:       "https://localhost:4444/foo/bar/baz.tar.gz",
			CgroupConfig: cgroups.Config{
				CPUs:   4,
				Memory: 1 * size.Gibibyte,
			},
		},
	}
	builder.SetLaunchables(launchables)
	err := builder.SetConfig(map[interface{}]interface{}{
		"ENVIRONMENT": "staging",
	})
	Assert(t).IsNil(err, "Should not have erred setting config for test manifest")
	builder.SetStatusPort(8000)

	manifest := builder.GetManifest()

	buff := bytes.Buffer{}
	manifest.Write(&buff)

	expected := testPod()
	Assert(t).AreEqual(buff.String(), expected, "Expected the manifest to marshal to the given yaml")
}

func TestPodManifestCanWriteItsConfigStanzaSeparately(t *testing.T) {
	config := testPod()
	manifest, err := ManifestFromBytes([]byte(config))
	Assert(t).IsNil(err, "should not have erred when building manifest")

	buff := bytes.Buffer{}
	err = manifest.WriteConfig(&buff)
	Assert(t).IsNil(err, "should not have erred when writing the config")
	expected := "ENVIRONMENT: staging\n"
	Assert(t).AreEqual(buff.String(), expected, "config should have been written")
}

func TestPodManifestCanReportItsSHA(t *testing.T) {
	config := testPod()
	manifest, err := ManifestFromBytes([]byte(config))
	Assert(t).IsNil(err, "should not have erred when building manifest")
	val, err := manifest.SHA()
	Assert(t).IsNil(err, "should not have erred when getting SHA")
	Assert(t).AreEqual(
		"fda3c8130dd7e2850b80bbbc0e56dcd3add02d4d67bed5cd3f8679db04854c58",
		val,
		"SHA mismatched expectations - if this was expected, change the assertion value",
	)
}

func TestPodManifestLaunchablesCGroups(t *testing.T) {
	config := testPod()
	manifest, _ := ManifestFromBytes([]byte(config))
	launchables := manifest.GetLaunchableStanzas()
	Assert(t).AreEqual(len(launchables), 1, "Expected exactly one launchable in the manifest")
	for _, launchable := range launchables {
		cgroup := launchable.CgroupConfig
		Assert(t).AreEqual(cgroup.CPUs, 4, "Expected cgroup to have 4 CPUs")
		Assert(t).AreEqual(cgroup.Memory, 1*size.Gibibyte, "Should have matched on memory bytecount")
	}
}

func TestNilPodManifestHasEmptySHA(t *testing.T) {
	var manifest *manifest
	content, err := manifest.SHA()
	Assert(t).IsNotNil(err, "Should have had an error when attempting to read SHA from nil manifest")
	Assert(t).AreEqual(content, "", "the SHA should have been empty")
}

func TestRunAs(t *testing.T) {
	config := testPod()
	manifest, err := ManifestFromBytes([]byte(config))
	Assert(t).IsNil(err, "should not have erred when building manifest")

	Assert(t).AreEqual(manifest.RunAsUser(), string(manifest.ID()), "RunAsUser() didn't match expectations")

	config += `run_as: specialuser`
	manifest, err = ManifestFromBytes([]byte(config))
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
	outBytes, err := manifest.Marshal()
	Assert(t).IsNil(err, "should not have erred extracting manifest struct to bytes")
	Assert(t).AreEqual(string(outBytes), string(manifestBytes), "Byte order should not have changed when unmarshaling and remarshaling a manifest")
}

func TestManifestBuilder(t *testing.T) {
	builder := NewManifestBuilder()
	builder.SetID("testpod")
	manifest := builder.GetManifest()

	Assert(t).AreEqual(string(manifest.ID()), "testpod", "id of built manifest did not match expected")
}

func TestManifestBuilderStripsFields(t *testing.T) {
	podManifest := manifest{
		raw:       []byte("foo"),
		signature: []byte("bar"),
		plaintext: []byte("baz"),
	}

	builder := podManifest.GetBuilder()
	builder.SetID("testpod")
	builtManifest := builder.GetManifest()

	plaintext, signature := builtManifest.SignatureData()
	Assert(t).AreEqual(len(plaintext), 0, "Expected plaintext to be zeroed when manifest is built")
	Assert(t).AreEqual(len(signature), 0, "Expected signature to be zeroed when manifest is built")
}

func TestBuilderHasOriginalFields(t *testing.T) {
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

	builder := manifest.GetBuilder()
	builtManifest := builder.GetManifest()
	Assert(t).AreEqual(string(builtManifest.ID()), "thepod", "Expected manifest ID to be preserved when converted to ManifestBuilder and back")
}

func TestGetConfigInitializesIfEmpty(t *testing.T) {
	builder := NewManifestBuilder()
	manifest := builder.GetManifest()
	config := manifest.GetConfig()
	Assert(t).IsNotNil(config, "Expected returned config to be instantiated by GetConfig() if not set")
}

func TestGetConfigReturnsCopy(t *testing.T) {
	builder := NewManifestBuilder()
	manifest := builder.GetManifest()
	config := manifest.GetConfig()
	config2 := manifest.GetConfig()
	config["foo"] = "bar"
	Assert(t).IsNil(config2["foo"], "config2 should have been unaffected by a change to config1")
}

type CantMarshal struct{}

var _ yaml.Marshaler = CantMarshal{}

func (CantMarshal) MarshalYAML() (interface{}, error) {
	return nil, errors.New("Can't yaml marshal this type")
}

func TestSetConfigErrsIfBadYAML(t *testing.T) {
	cantMarshalConfig := make(map[interface{}]interface{})
	cantMarshalConfig["foo"] = CantMarshal{}

	builder := NewManifestBuilder()
	err := builder.SetConfig(cantMarshalConfig)
	Assert(t).IsNotNil(err, "Should have erred setting config with a type that cannot be marshaled as YAML")
}

func TestSetConfigCopies(t *testing.T) {
	builder := NewManifestBuilder()
	config := map[interface{}]interface{}{
		"foo": "bar",
	}

	err := builder.SetConfig(config)
	Assert(t).IsNil(err, "Should not have errored setting config")
	manifest := builder.GetManifest()
	manifestConfig := manifest.GetConfig()
	Assert(t).AreEqual(manifestConfig["foo"], "bar", "Should have been able to read config values out unchanged after setting them")
	config["foo"] = "baz"
	Assert(t).AreEqual(manifestConfig["foo"], "bar", "Config values shouldn't have changed when mutating the original input due to deep copy")
}
