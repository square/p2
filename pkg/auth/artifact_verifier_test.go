package auth

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

func TestManifestVerifierAuthorizesValidBuild(t *testing.T) {
	artifactDir := util.From(runtime.Caller(0)).ExpandPath("test_artifact")
	verifier, err := NewBuildArtifactVerifier(filepath.Join(artifactDir, "public.key"), uri.DefaultFetcher, &logging.DefaultLogger)
	if err != nil {
		t.Fatalf("Error getting public key: %v", err)
	}

	location := filepath.Join(artifactDir, "hello-server_3881c78ed47ae8be4a4080178f2d46cc174a5a95.tar.gz")
	file, err := os.Open(location)
	if err != nil {
		t.Fatalf("Error reading hello tar: %v", err)
	}
	if err = verifier.VerifyHoistArtifact(file, "file://"+location); err != nil {
		t.Fatalf("Could not verify artifact integrity: %v", err)
	}
}
