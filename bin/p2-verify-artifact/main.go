package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/version"
)

var (
	location         = kingpin.Arg("location", "The URI of the artifact.").Required().String()
	originalLocation = kingpin.Flag("original-location", "The URI where the artifact which has already been downloaded came from. The primary location must be an existing file").String()
	gpgKeyringPath   = kingpin.Flag("keyring", "The PGP keyring to use to verify the artifact").Required().ExistingFile()
)

func main() {
	kingpin.Version(version.VERSION)
	k := kingpin.Parse()
	log.Println(k)

	verifier, err := auth.NewBuildArtifactVerifier(*gpgKeyringPath, uri.DefaultFetcher, &logging.DefaultLogger)
	if err != nil {
		log.Fatalln(err)
	}

	dir, err := ioutil.TempDir("", "verify")
	defer os.RemoveAll(dir)
	if err != nil {
		log.Fatalf("Could not create tempdir for artifact download: %v", err)
	}

	localCopyPath := filepath.Join(dir, "temp.tar.gz")

	var localCopy *os.File
	var locationForSignature string

	if *originalLocation == "" {
		err = uri.DefaultFetcher.CopyLocal(*location, localCopyPath)
		if err != nil {
			log.Fatalln(err)
		}
		localCopy, err = os.Open(localCopyPath)
		if err != nil {
			log.Fatalf("Could not open local copy of the file %v: %v", localCopyPath, err)
		}
		locationForSignature = *location
	} else {
		localCopy, err = os.Open(*location)
		if err != nil {
			log.Fatalf("Could not open local copy of the file %v: %v", *location, err)
		}
		locationForSignature = *originalLocation
	}

	if err = verifier.VerifyHoistArtifact(localCopy, locationForSignature); err != nil {
		log.Fatalln(err)
	}

	log.Println("OK")
	os.Exit(0)
}
