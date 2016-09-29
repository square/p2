package auth

import (
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

type testFile string

var (
	testArtifact    testFile = "hello-server_3881c78ed47ae8be4a4080178f2d46cc174a5a95.tar.gz"
	testBuildSig    testFile = "hello-server_3881c78ed47ae8be4a4080178f2d46cc174a5a95.tar.gz.sig"
	testManifest    testFile = "hello-server_3881c78ed47ae8be4a4080178f2d46cc174a5a95.tar.gz.manifest"
	testManifestSig testFile = "hello-server_3881c78ed47ae8be4a4080178f2d46cc174a5a95.tar.gz.manifest.sig"
)

func buildTestFileTree(t *testing.T, files []testFile) string {
	tempDir, err := ioutil.TempDir("", "test-artifact-verifier")
	if err != nil {
		t.Fatalf("Could not make tempdir for verification: %v", err)
	}
	artifactDir := util.From(runtime.Caller(0)).ExpandPath("test_artifact")
	for _, file := range files {
		err = os.Link(filepath.Join(artifactDir, string(file)), filepath.Join(tempDir, string(file)))
		if err != nil {
			t.Fatal(err)
		}
	}
	return tempDir
}

// This is copied from pkg/artifact/location_data.go to avoid an import cycle
func VerificationDataForLocation(location *url.URL) VerificationData {
	manifestLocation := &url.URL{}
	*manifestLocation = *location
	manifestLocation.Path = location.Path + ".manifest"

	manifestSignatureLocation := &url.URL{}
	*manifestSignatureLocation = *manifestLocation
	manifestSignatureLocation.Path = manifestLocation.Path + ".sig"

	buildSignatureLocation := &url.URL{}
	*buildSignatureLocation = *location
	buildSignatureLocation.Path = location.Path + ".sig"
	return VerificationData{
		ManifestLocation:          manifestLocation,
		ManifestSignatureLocation: manifestSignatureLocation,
		BuildSignatureLocation:    buildSignatureLocation,
	}
}

func testVerifiedWithFiles(t *testing.T, files []testFile, verifier ArtifactVerifier) {
	testDir := buildTestFileTree(t, files)
	defer os.RemoveAll(testDir)
	filePath := filepath.Join(testDir, string(testArtifact))
	localCopy, err := os.Open(filePath)
	if err != nil {
		t.Fatal(err)
	}

	url := &url.URL{
		Scheme: "file",
		Path:   filePath,
	}
	verificationData := VerificationDataForLocation(url)

	err = verifier.VerifyHoistArtifact(localCopy, verificationData)
	if err != nil {
		t.Fatalf("Expected files %v to pass verification, got: %v", files, err)
	}
}

func testNotVerifiedWithFiles(t *testing.T, files []testFile, verifier ArtifactVerifier) {
	testDir := buildTestFileTree(t, files)
	defer os.RemoveAll(testDir)
	filePath := filepath.Join(testDir, string(testArtifact))
	localCopy, err := os.Open(filePath)
	if err != nil {
		t.Fatal(err)
	}

	url := &url.URL{
		Scheme: "file",
		Path:   filePath,
	}

	verificationData := VerificationDataForLocation(url)

	err = verifier.VerifyHoistArtifact(localCopy, verificationData)
	if err == nil {
		t.Fatal("Expected files to fail verification, but didn't")
	}
}

func testKeyringPath() string {
	artifactDir := util.From(runtime.Caller(0)).ExpandPath("test_artifact")
	return filepath.Join(artifactDir, "public.key")
}

func TestManifestVerifierAuthorizesValidBuild(t *testing.T) {
	verifier, err := NewBuildManifestVerifier(testKeyringPath(), uri.DefaultFetcher, &logging.DefaultLogger)
	if err != nil {
		t.Fatalf("Error getting public key: %v", err)
	}
	testVerifiedWithFiles(t, []testFile{testArtifact, testManifest, testManifestSig}, verifier)
}

func TestBuildVerifierAuthorizesValidBuild(t *testing.T) {
	verifier, err := NewBuildVerifier(testKeyringPath(), uri.DefaultFetcher, &logging.DefaultLogger)
	if err != nil {
		t.Fatalf("Error getting public key: %v", err)
	}
	testVerifiedWithFiles(t, []testFile{testArtifact, testBuildSig}, verifier)
}

func TestManifestVerifierFailsValidBuildWithoutSig(t *testing.T) {
	verifier, err := NewBuildManifestVerifier(testKeyringPath(), uri.DefaultFetcher, &logging.DefaultLogger)
	if err != nil {
		t.Fatalf("Error getting public key: %v", err)
	}
	testNotVerifiedWithFiles(t, []testFile{testArtifact, testManifest}, verifier)
}

func TestBuildVerifierFailsValidBuildWithoutSig(t *testing.T) {
	verifier, err := NewBuildVerifier(testKeyringPath(), uri.DefaultFetcher, &logging.DefaultLogger)
	if err != nil {
		t.Fatalf("Error getting public key: %v", err)
	}
	testNotVerifiedWithFiles(t, []testFile{testArtifact}, verifier)
}

func TestCompositeVerifierAuthorizesBuildWithBuildSig(t *testing.T) {
	verifier, err := NewCompositeVerifier(testKeyringPath(), uri.DefaultFetcher, &logging.DefaultLogger)
	if err != nil {
		t.Fatalf("Error getting public key: %v", err)
	}
	testVerifiedWithFiles(t, []testFile{testArtifact, testBuildSig}, verifier)
}

func TestCompositeVerifierAuthorizesBuildWithManifestSig(t *testing.T) {
	verifier, err := NewCompositeVerifier(testKeyringPath(), uri.DefaultFetcher, &logging.DefaultLogger)
	if err != nil {
		t.Fatalf("Error getting public key: %v", err)
	}
	testVerifiedWithFiles(t, []testFile{testArtifact, testManifest, testManifestSig}, verifier)
}

func TestCompositeVerifierFailsBuildWithoutSigs(t *testing.T) {
	verifier, err := NewCompositeVerifier(testKeyringPath(), uri.DefaultFetcher, &logging.DefaultLogger)
	if err != nil {
		t.Fatalf("Error getting public key: %v", err)
	}
	testNotVerifiedWithFiles(t, []testFile{testArtifact, testManifest}, verifier)
}
