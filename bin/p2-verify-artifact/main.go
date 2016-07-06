package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/version"
)

var (
	location         = kingpin.Arg("location", "The path to the artifact.").Required().String()
	originalLocation = kingpin.Flag("original-location", "The URI where the artifact which has already been downloaded came from. The primary location must be an existing file").URL()
	gpgKeyringPath   = kingpin.Flag("keyring", "The PGP keyring to use to verify the artifact").Required().ExistingFile()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	dir, err := ioutil.TempDir("", "verify")
	defer os.RemoveAll(dir)
	if err != nil {
		log.Fatalf("Could not create tempdir for artifact download: %v", err)
	}

	localCopyPath := filepath.Join(dir, "temp.tar.gz")

	locationForSignature := *originalLocation

	var localCopy *os.File
	if *originalLocation != nil {
		err = uri.DefaultFetcher.CopyLocal(*originalLocation, localCopyPath)
		if err != nil {
			log.Fatalln(err)
		}
		localCopy, err = os.Open(localCopyPath)
		if err != nil {
			log.Fatalf("Could not open local copy of the file %v: %v", localCopyPath, err)
		}
	} else {
		localCopy, err = os.Open(*location)
		if err != nil {
			log.Fatalf("Could not open local copy of the file %v: %v", *location, err)
		}
	}

	res := struct {
		SignedManifest bool   `json:"signed_manifest"`
		SignedBuild    bool   `json:"signed_build"`
		ManifestErr    string `json:"manifest_error,omitempty"`
		BuildErr       string `json:"build_error,omitempty"`
	}{}

	verificationData := artifact.VerificationDataForLocation(locationForSignature)
	manifestVerifier, buildErr := auth.NewBuildManifestVerifier(*gpgKeyringPath, uri.DefaultFetcher, &logging.DefaultLogger)
	buildVerifier, manErr := auth.NewBuildVerifier(*gpgKeyringPath, uri.DefaultFetcher, &logging.DefaultLogger)

	if buildErr == nil {
		err := buildVerifier.VerifyHoistArtifact(localCopy, verificationData)
		if err == nil {
			res.SignedBuild = true
		} else {
			res.BuildErr = err.Error()
		}
	} else {
		res.BuildErr = buildErr.Error()
	}

	_, _ = localCopy.Seek(0, os.SEEK_SET)

	if manErr == nil {
		err := manifestVerifier.VerifyHoistArtifact(localCopy, verificationData)
		if err == nil {
			res.SignedManifest = true
		} else {
			res.ManifestErr = err.Error()
		}
	} else {
		res.ManifestErr = manErr.Error()
	}

	marshaled, _ := json.Marshal(res)
	fmt.Println(string(marshaled))
	os.Exit(0)
}
