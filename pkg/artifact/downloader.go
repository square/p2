package artifact

import (
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"

	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/gzip"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

// Interface for downloading a single artifact.
type Downloader interface {
	// Downloads the artifact represented by the Downloader to the
	// specified path and transfers file ownership to the specified user
	Download(location *url.URL, verificationData auth.VerificationData, destination string, owner string) error
}

// Implements the Downloader interface. Simply fetches a .tar.gz file from a
// configured URL and extracts it to the location passed to DownloadTo
type downloader struct {
	fetcher  uri.Fetcher
	verifier auth.ArtifactVerifier
}

func NewLocationDownloader(fetcher uri.Fetcher, verifier auth.ArtifactVerifier) Downloader {
	return &downloader{
		fetcher:  fetcher,
		verifier: verifier,
	}
}

func (l *downloader) Download(location *url.URL, verificationData auth.VerificationData, dst string, owner string) error {
	// Write to a temporary file for easy cleanup if the network transfer fails
	// TODO: the end of the artifact URL may not always be suitable as a directory
	// name
	artifactFile, err := ioutil.TempFile("", filepath.Base(location.Path))
	if err != nil {
		return err
	}
	defer os.Remove(artifactFile.Name())
	defer artifactFile.Close()

	remoteData, err := l.fetcher.Open(location)
	if err != nil {
		return err
	}
	defer remoteData.Close()
	_, err = io.Copy(artifactFile, remoteData)
	if err != nil {
		return util.Errorf("Could not copy artifact locally: %v", err)
	}
	// rewind once so we can ask the verifier
	_, err = artifactFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return util.Errorf("Could not reset artifact file position for verification: %v", err)
	}

	err = l.verifier.VerifyHoistArtifact(artifactFile, verificationData)
	if err != nil {
		return err
	}

	// rewind a second time to allow the archive to be unpacked
	_, err = artifactFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return util.Errorf("Could not reset artifact file position after verification: %v", err)
	}

	err = gzip.ExtractTarGz(owner, artifactFile, dst)
	if err != nil {
		_ = os.RemoveAll(dst)
		return util.Errorf("error while extracting artifact: %s", err)
	}
	return err
}
