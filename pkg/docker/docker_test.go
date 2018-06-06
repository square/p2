package docker

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvVars(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "test_load_env_vars")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	podEnvDir := filepath.Join(tempDir, "pod")
	launchableEnvDir := filepath.Join(tempDir, "launchable")
	err = os.Mkdir(podEnvDir, 0755)
	if err != nil {
		t.Fatal(err)
	}
	err = os.Mkdir(launchableEnvDir, 0755)
	if err != nil {
		t.Fatal(err)
	}

	err = ioutil.WriteFile(filepath.Join(podEnvDir, "OVERRIDE"), []byte("not_overridden"), 0644)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile(filepath.Join(launchableEnvDir, "OVERRIDE"), []byte("overridden"), 0644)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile(filepath.Join(launchableEnvDir, "LAUNCHABLE_ONLY"), []byte("whatever"), 0644)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile(filepath.Join(podEnvDir, "POD_ONLY"), []byte("whatever"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	envVars, err := loadEnvVars(podEnvDir, launchableEnvDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(envVars) != 3 {
		t.Fatalf("expected 3 env vars but there were %d", len(envVars))
	}

	if !contains(envVars, "OVERRIDE=overridden") {
		t.Errorf("expected to find OVERRIDE=overridden in the environment variables because the launchable one should override the pod one, found %s", envVars)
	}
	if !contains(envVars, "POD_ONLY=whatever") {
		t.Errorf("expected to find POD_ONLY=whatever in the environment variables because the launchable one should override the pod one, found %s", envVars)
	}
	if !contains(envVars, "LAUNCHABLE_ONLY=whatever") {
		t.Errorf("expected to find LAUNCHABLE_ONLY=whatever in the environment variables because the launchable one should override the pod one, found %s", envVars)
	}
}

func contains(set []string, val string) bool {
	for _, s := range set {
		if s == val {
			return true
		}
	}

	return false
}
