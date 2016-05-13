package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"

	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/openpgp"
	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/crypto/openpgp/armor"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/yaml.v2"
)

const VerifyNone = "none"
const VerifyManifest = "manifest"

// The artifact verifier is responsible for checking that the artifact
// was created by a trusted entity.
type ArtifactVerifier interface {
	VerifyHoistArtifact(localCopy *os.File, artifactLocation string) error
}

type noopVerifier struct{}

func (n *noopVerifier) VerifyHoistArtifact(_ *os.File, _ string) error {
	return nil
}

func NoopVerifier() ArtifactVerifier {
	return &noopVerifier{}
}

// BuildManifestVerifier ensures that the given launchable's location
// field is matched with a corresponding manifest certifying the validity
// of the build. The manifest is a YAML file containing a single key "artifact_sha".
// That key represents the hex digest of the artifact tar itself. The manifest is
// signed by the build system and has a corresponding URL for downloading the signature.
//
// The manifest and signature files should match the following convention if the launchable
// location is a full path to a file:
//
// If the artifact is located here:
// https://foo.bar.baz/artifacts/myapp_abc123.tar.gz
//
// Then its build manifest is located here:
// https://foo.bar.baz/artifacts/myapp_abc123.tar.gz.manifest
//
// artifact_sha: abc23456
//
// And its signature file is located here:
// https://foo.bar.baz/artifacts/myapp_abc123.tar.gz.manifest.sig
type BuildManifestVerifier struct {
	keyring openpgp.KeyRing
	fetcher uri.Fetcher
	logger  *logging.Logger
}

func NewBuildArtifactVerifier(keyringPath string, fetcher uri.Fetcher, logger *logging.Logger) (*BuildManifestVerifier, error) {
	keyring, err := LoadKeyring(keyringPath)
	if err != nil {
		return nil, util.Errorf("Could not load artifact verification keyring from %v: %v", keyringPath, err)
	}
	return &BuildManifestVerifier{
		keyring: keyring,
		fetcher: fetcher,
		logger:  logger,
	}, nil
}

// Returns an error if the stanza's artifact is not signed appropriately. Note that this
// implementation does not use the pod manifest digest location options.
func (b *BuildManifestVerifier) VerifyHoistArtifact(localCopy *os.File, artifactLocation string) error {
	u, err := url.Parse(artifactLocation)
	if err != nil {
		return err
	}
	switch u.Scheme {
	default:
		return fmt.Errorf("%v does not have a recognized scheme, cannot verify manifest or signature", artifactLocation)
	case "http", "https", "file":
		dir, err := ioutil.TempDir("", "artifact_verification")
		if err != nil {
			return fmt.Errorf("Could not create temporary directory for manifest file: %v", err)
		}
		defer os.RemoveAll(dir)

		manifestSrc := fmt.Sprintf("%v.manifest", artifactLocation)
		manifestDst := filepath.Join(dir, "manifest")
		err = b.fetcher.CopyLocal(manifestSrc, manifestDst)
		if err != nil {
			return fmt.Errorf("Could not download artifact manifest for %v: %v", artifactLocation, err)
		}

		signatureSrc := fmt.Sprintf("%v.sig", manifestSrc)
		signatureDst := filepath.Join(dir, "signature")
		if err = b.fetcher.CopyLocal(signatureSrc, signatureDst); err != nil {
			return fmt.Errorf("Could not download manifest signature for %v: %v", artifactLocation, err)
		}

		manifestBytes, err := ioutil.ReadFile(manifestDst)
		if err != nil {
			return err
		}
		signatureBytes, err := ioutil.ReadFile(signatureDst)
		if err != nil {
			return err
		}

		if err = b.verifySigned(manifestBytes, signatureBytes); err != nil {
			return err
		}

		return b.checkMatchingDigest(localCopy, manifestBytes)
	}
}

func (b *BuildManifestVerifier) verifySigned(manifestBytes, signatureBytes []byte) error {
	// permit an armored detached signature
	block, err := armor.Decode(bytes.NewBuffer(signatureBytes))
	if err == nil {
		signatureBytes, err = ioutil.ReadAll(block.Body)
		if err != nil {
			return err
		}
	}
	// check that the manifest was adequately signed by our signer
	_, err = checkDetachedSignature(b.keyring, manifestBytes, signatureBytes)
	if err != nil {
		return fmt.Errorf("Could not verify artifact manifest against the signature: %v", err)
	}
	return nil
}

func (b *BuildManifestVerifier) checkMatchingDigest(localCopy *os.File, manifestBytes []byte) error {
	realTarBytes, err := ioutil.ReadAll(localCopy)
	if err != nil {
		return fmt.Errorf("Could not read given local copy of the artifact: %v", err)
	}
	digestBytes := sha256.Sum256(realTarBytes)
	realDigest := hex.EncodeToString(digestBytes[:])

	manifest := struct {
		ArtifactDigest string `yaml:"artifact_sha"`
	}{}
	err = yaml.Unmarshal(manifestBytes, &manifest)
	if err != nil {
		return fmt.Errorf("Could not unmarshal manifest bytes: %v", err)
	}

	if realDigest != manifest.ArtifactDigest {
		return fmt.Errorf("Artifact hex digest did not match the given manifest: expected %v, was actually %v", realDigest, manifest.ArtifactDigest)
	}
	return nil
}
