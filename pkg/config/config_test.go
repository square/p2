package config

import (
	"os"
	"path"
	"runtime"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func testFilePath() string {
	_, filename, _, _ := runtime.Caller(1)
	return path.Join(path.Dir(filename), "fake_config_file.yaml")
}

func readTestFile() *Config {
	cfg, err := LoadConfigFile(testFilePath())
	if err != nil {
		panic(err.Error())
	}
	return cfg
}

func TestConfigFileCanReadStringKeys(t *testing.T) {
	app, err := readTestFile().ReadString("app")
	Assert(t).IsNil(err, "app should have been a valid string key")

	if app != "multicurse" {
		t.Fatal("Expected config to be able to read the app name")
	}
}

func TestReadStringSlice(t *testing.T) {
	s, err := readTestFile().ReadStringSlice("slice")
	Assert(t).IsNil(err, "slice should have been a valid string slice key")
	Assert(t).AreEqual(3, len(s), "Expected 3 entries")
	Assert(t).AreEqual("a", s[0], "First entry a")
	Assert(t).AreEqual("b", s[1], "Second entry b")
	Assert(t).AreEqual("c", s[2], "Third entry c")
}

func TestReadBool(t *testing.T) {
	conf, err := readTestFile().ReadMap("bool")
	Assert(t).IsNil(err, "bool should be a map")

	v, err := conf.ReadBool("t")
	if err != nil {
		panic(err.Error())
	}
	Assert(t).AreEqual(v, true, "Mismatch")

	v, err = conf.ReadBool("f")
	if err != nil {
		panic(err.Error())
	}
	Assert(t).AreEqual(v, false, "Mismatch")

	v, err = conf.ReadBool("default")
	// Expected error, defaulting to false
	if err == nil {
		panic(err.Error())
	}
	Assert(t).AreEqual(v, false, "Mismatch")

}

func TestConfigCanBeReadFromEnvironment(t *testing.T) {
	prev := os.Getenv("CONFIG_PATH")
	os.Setenv("CONFIG_PATH", testFilePath())
	defer os.Setenv("CONFIG_PATH", prev)

	cfg, err := LoadFromEnvironment()
	Assert(t).IsNil(err, "An error occurred while trying to load the test configuration")

	app, err := cfg.ReadString("app")

	Assert(t).IsNil(err, "Expected app to be a valid key read by string")
	Assert(t).AreEqual(app, "multicurse", "Expected environment-backed config to be able to read the app name")
}

func TestConfigDeterministicKeyOrdering(t *testing.T) {
	conf, err := readTestFile().ReadMap("foomap")
	Assert(t).IsNil(err, "foomap should be a map")
	keys := conf.Keys()
	Assert(t).AreEqual(keys[0], "a", "key a should have come first")
	Assert(t).AreEqual(keys[1], "c", "key c should have come second")
}
