package digest

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

const hashLength = sha256.Size * 2 // the length in hexadecimal for a sha256 hash

type Digest struct {
	// A map of file path -> hash
	FileHashes map[string]string
	plaintext  []byte
	signature  []byte
}

func (digest Digest) SignatureData() (plaintext, signature []byte) {
	if digest.signature == nil {
		return nil, nil
	}
	return digest.plaintext, digest.signature
}

func (digest Digest) VerifyDir(root string) error {
	return VerifyDir(root, digest.FileHashes)
}

// Walks an entire file tree and takes the sha256sum of every file, comparing it
// to a digest containing all the expected sha256sums. If any file's hash does
// not match, or if there are files missing (from the digest or the tree),
// an error is returned.
func VerifyDir(root string, digest map[string]string) error {
	foundFiles := map[string]bool{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			// some particularly misbehaved tarballs contain absolute symlinks
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if filepath.IsAbs(linkTarget) {
				// we can't fix them at this point, and statting them will blow
				// up, so let's bail for now
				return nil
			}

			// walk does not follow links, so this symlink might actually point
			// to a directory
			// we want to skip that
			linkInfo, err := os.Stat(path)
			if err != nil {
				return err
			}
			if linkInfo.IsDir() {
				return nil
			}
		}

		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		expectedSHA, ok := digest[relPath]
		if !ok {
			return util.Errorf("File %s (full path %s) was not in digest", relPath, path)
		}
		// `foundFiles` is always a subset of `digest`
		foundFiles[relPath] = true

		fd, err := os.Open(path)
		if err != nil {
			return err
		}
		defer fd.Close()

		hasher := sha256.New()
		_, err = io.Copy(hasher, fd)
		if err != nil {
			return err
		}
		receivedSHA := hex.EncodeToString(hasher.Sum(nil))

		if receivedSHA != expectedSHA {
			return util.Errorf("Received SHA %s (expected %s) for file %s", receivedSHA, expectedSHA, path)
		}
		return nil
	})

	if err != nil {
		return err
	}
	// By construction `foundFiles` is a subset of `digest`, so this
	// is a faster check for equivalence.
	if len(digest) != len(foundFiles) {
		return util.Errorf("Not all files in the digest were found")
	}
	return nil
}

// Parses a sha256sum digest at the given digest URI, with a detached
// PGP signature at the given signature URI. If the pod manifest does
// not declare a signature path, use "".
func ParseUris(
	fetcher uri.Fetcher,
	digestUri string,
	signatureUri string,
) (Digest, error) {
	digest, err := fetcher.Open(digestUri)
	if err != nil {
		return Digest{}, err
	}
	defer digest.Close()
	var signature io.ReadCloser
	if signatureUri != "" {
		signature, err = fetcher.Open(signatureUri)
		if err != nil {
			return Digest{}, err
		}
		defer signature.Close()
	}
	return Parse(digest, signature)
}

// Parses a sha256sum digest. Each line is a 64-character sha256 hash
// in lowercase hexadecimal, followed by a space, followed by an
// unused mode character, followed by a filename. The signature is
// optional.
func Parse(digestReader, signatureReader io.Reader) (Digest, error) {
	// Keep a copy of the digest text and signature so the signature can be verified.
	digestBuf := bytes.NewBuffer([]byte{})
	_, err := digestBuf.ReadFrom(digestReader)
	if err != nil {
		return Digest{}, err
	}
	digest := digestBuf.Bytes()
	var signature []byte
	if signatureReader != nil {
		sigBuf := bytes.NewBuffer([]byte{})
		_, err := sigBuf.ReadFrom(signatureReader)
		if err != nil {
			return Digest{}, err
		}
		signature = sigBuf.Bytes()
	}

	scanner := bufio.NewScanner(bytes.NewReader(digest))
	scanner.Split(bufio.ScanLines)

	ret := map[string]string{}
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		if len(line) <= hashLength+2 {
			// this line could not have been produced by a sha1sum
			return Digest{}, util.Errorf("invalid shasum output: %q", line)
		}

		filename := line[hashLength+2:]
		cleanname := filepath.Clean(filename)
		if _, ok := ret[cleanname]; ok {
			return Digest{}, util.Errorf("duplicate filename: %q", filename)
		}
		ret[cleanname] = line[:hashLength]
	}
	return Digest{ret, digest, signature}, nil
}
