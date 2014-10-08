package config

import (
	"os"
	"path"
	"runtime"
	"testing"
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
	app := readTestFile().ReadString("app")
	if app != "multicurse" {
		t.Fatal("Expected config to be able to read the app name")
	}
}

func TestConfigCanBeReadFromEnvironment(t *testing.T) {
	prev := os.Getenv("CONFIG_PATH")
	os.Setenv("CONFIG_PATH", testFilePath())
	defer os.Setenv("CONFIG_PATH", prev)

	cfg, err := LoadFromEnvironment()
	if err != nil {
		t.Fatalf("An error occurred while trying to load the test configuration: %s", err)
	}
	app := cfg.ReadString("app")
	if app != "multicurse" {
		t.Fatal("Expected environment-backed config to be able to read the app name")
	}
}
