package manifest

import (
	"bytes"
	"errors"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/util/size"

	. "github.com/anthonybishopric/gotcha"
	"gopkg.in/yaml.v2"
)

func TestPodManifestCanBeRead(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	testPath := filepath.Join(filepath.Dir(filename), "test_manifest.yaml")

	manifest, err := FromPath(testPath)
	Assert(t).IsNil(err, "Should not have failed to get pod manifest.")
	Assert(t).AreEqual("hello", string(manifest.ID()), "Id read from manifest didn't have expected value")
	Assert(t).AreEqual(manifest.GetLaunchableStanzas()["app"].Location, "hoisted-hello_def456.tar.gz", "Location read from manifest didn't have expected value")
	Assert(t).AreEqual("hoist", manifest.GetLaunchableStanzas()["app"].LaunchableType, "LaunchableType read from manifest didn't have expected value")

	Assert(t).AreEqual(4, manifest.GetResourceLimits().CGroup.CPUs, "CPU count for pod level cgroup didn't have expected value")

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
    cgroup:
      cpus: 4
      memory: 1073741824
    location: https://localhost:4444/foo/bar/baz.tar.gz
resource_limits:
  cgroup:
    cpus: 8
    memory: 1099511627776
config:
  ENVIRONMENT: staging
status:
  port: 8000
`
}

func testPodOldStatus() string {
	return `id: thepod
launchables:
  my-app:
    launchable_type: hoist
    cgroup:
      cpus: 4
      memory: 1073741824
    location: https://localhost:4444/foo/bar/baz.tar.gz
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
	builder := NewBuilder()
	builder.SetID("thepod")
	launchables := map[launch.LaunchableID]launch.LaunchableStanza{
		"my-app": {
			LaunchableType: "hoist",
			Location:       "https://localhost:4444/foo/bar/baz.tar.gz",
			CgroupConfig: cgroups.Config{
				CPUs:   4,
				Memory: 1 * size.Gibibyte,
			},
		},
	}
	builder.SetPodLevelCgroup(cgroups.Config{
		CPUs:   8,
		Memory: 1 * size.Tebibyte,
	})
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
	manifest, err := FromBytes([]byte(config))
	Assert(t).IsNil(err, "should not have erred when building manifest")

	buff := bytes.Buffer{}
	err = manifest.WriteConfig(&buff)
	Assert(t).IsNil(err, "should not have erred when writing the config")
	expected := "ENVIRONMENT: staging\n"
	Assert(t).AreEqual(buff.String(), expected, "config should have been written")
}

func TestPodManifestCanReportItsSHA(t *testing.T) {
	config := testPodOldStatus()
	manifest, err := FromBytes([]byte(config))
	Assert(t).IsNil(err, "should not have erred when building manifest")
	val, err := manifest.SHA()
	Assert(t).IsNil(err, "should not have erred when getting SHA")
	expected := "f7fdad6e2362c9345a83196701dafb989fa8229b8d671642976cb35b5166c6f0"
	if val != expected {
		t.Errorf("Expected manifest sha to be %s but was %s. If this was expected, change the assertion value", expected, val)
	}
}

func TestPodManifestLaunchablesCGroups(t *testing.T) {
	config := testPod()
	manifest, _ := FromBytes([]byte(config))
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

func TestStatusHTTP(t *testing.T) {
	tests := []struct {
		config   string
		expected bool
	}{
		{`{ id: thepod }`, false},
		{`{ id: thepod, status_http: false }`, false},
		{`{ id: thepod, status_http: true }`, true},
		{`{ id: thepod, status: { http: false }, status_http: true }`, true},
		{`{ id: thepod, status: { http: true } }`, true},
	}
	for _, test := range tests {
		manifest, err := FromBytes([]byte(test.config))
		Assert(t).IsNil(err, "should not have erred when building manifest")

		Assert(t).AreEqual(test.expected, manifest.GetStatusHTTP(), "uses the correct protocol")
	}
}

func TestStatusPath(t *testing.T) {
	tests := []struct {
		config   string
		expected string
	}{
		{`{ id: thepod, status: { port: 5 } }`, "/_status"},
		{`{ id: thepod, status: { path: _status } }`, "/_status"},
		{`{ id: thepod, status: { path: _foobar } }`, "/_foobar"},
		{`{ id: thepod, status: { path: /_foobar } }`, "/_foobar"},
	}
	for _, test := range tests {
		manifest, err := FromBytes([]byte(test.config))
		Assert(t).IsNil(err, "should not have erred when building manifest")

		Assert(t).AreEqual(test.expected, manifest.GetStatusPath(), "uses the correct path")
	}
}

func TestStatusPort(t *testing.T) {
	tests := []struct {
		config   string
		expected int
	}{
		{`{ id: thepod, status_port: 555 }`, 555},
		{`{ id: thepod }`, 0},
		{`{ id: thepod, status: { port: 123 }, status_port: 456 }`, 456},
		{`{ id: thepod, status: { port: 398 } }`, 398},
	}
	for _, test := range tests {
		manifest, err := FromBytes([]byte(test.config))
		Assert(t).IsNil(err, "should not have erred when building manifest")

		Assert(t).AreEqual(test.expected, manifest.GetStatusPort(), "uses the correct port")
	}
}

func TestRunAs(t *testing.T) {
	config := testPod()
	manifest, err := FromBytes([]byte(config))
	Assert(t).IsNil(err, "should not have erred when building manifest")

	Assert(t).AreEqual(manifest.RunAsUser(), string(manifest.ID()), "RunAsUser() didn't match expectations")

	config += `run_as: specialuser`
	manifest, err = FromBytes([]byte(config))
	Assert(t).IsNil(err, "should not have erred when building manifest")
	Assert(t).AreEqual(manifest.RunAsUser(), "specialuser", "RunAsUser() didn't match expectations")
}

func TestByteOrderPreserved(t *testing.T) {
	// The yaml keys here are intentionally ordered in a way that without special
	// care, the bytes returned by manifest.Bytes() would be in a different order
	// than the bytes passed in to FromBytes()
	manifestBytes := []byte(`id: thepod
launchables:
  my-app:
    launchable_type: hoist
    location: https://localhost:4444/foo/bar/baz.tar.gz
status_port: 8000
config:
  ENVIRONMENT: staging
`)
	manifest, err := FromBytes(manifestBytes)
	Assert(t).IsNil(err, "should not have erred constructing manifest from bytes")
	outBytes, err := manifest.Marshal()
	Assert(t).IsNil(err, "should not have erred extracting manifest struct to bytes")
	Assert(t).AreEqual(string(outBytes), string(manifestBytes), "Byte order should not have changed when unmarshaling and remarshaling a manifest")
}

func TestBuilder(t *testing.T) {
	builder := NewBuilder()
	builder.SetID("testpod")
	manifest := builder.GetManifest()

	Assert(t).AreEqual(string(manifest.ID()), "testpod", "id of built manifest did not match expected")
}

func TestBuilderStripsFields(t *testing.T) {
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
    location: https://localhost:4444/foo/bar/baz.tar.gz
status_port: 8000
config:
  ENVIRONMENT: staging
`)
	manifest, err := FromBytes(manifestBytes)
	Assert(t).IsNil(err, "should not have erred constructing manifest from bytes")

	builder := manifest.GetBuilder()
	builtManifest := builder.GetManifest()
	Assert(t).AreEqual(string(builtManifest.ID()), "thepod", "Expected manifest ID to be preserved when converted to Builder and back")
}

func TestGetConfigInitializesIfEmpty(t *testing.T) {
	builder := NewBuilder()
	manifest := builder.GetManifest()
	config := manifest.GetConfig()
	Assert(t).IsNotNil(config, "Expected returned config to be instantiated by GetConfig() if not set")
}

func TestGetConfigReturnsCopy(t *testing.T) {
	builder := NewBuilder()
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

	builder := NewBuilder()
	err := builder.SetConfig(cantMarshalConfig)
	Assert(t).IsNotNil(err, "Should have erred setting config with a type that cannot be marshaled as YAML")
}

func TestSetConfigCopies(t *testing.T) {
	builder := NewBuilder()
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
