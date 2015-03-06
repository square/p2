package digest

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/square/p2/pkg/util"
	"golang.org/x/crypto/openpgp"
)

const hashLength = sha256.Size * 2 // the length in hexadecimal for a sha256 hash

// Walks an entire file tree and takes the sha256sum of every file, comparing it
// to a digest containing all the expected sha256sums. If any file's hash does
// not match, or if there are files missing (from the digest or the tree),
// an error is returned.
func VerifyDir(root string, digest map[string]string) error {
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		// walk does not follow links, so if this is a symlink to a directory,
		// we should skip that
		if info.Mode()&os.ModeSymlink != 0 {
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
		delete(digest, relPath)

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
	if len(digest) != 0 {
		return util.Errorf("Not all files in the digest were found")
	}
	return nil
}

// Parses a sha256sum digest at the given digest path, with a detached PGP
// signature at the given signature path. The signer's public key should be on
// the given keyring.
func ParseSignedDigestFiles(digestPath string, signaturePath string, keyring openpgp.KeyRing) (map[string]string, error) {
	digest, err := os.Open(digestPath)
	if err != nil {
		return nil, err
	}
	defer digest.Close()
	signature, err := os.Open(signaturePath)
	if err != nil {
		return nil, err
	}
	defer signature.Close()

	signer, err := openpgp.CheckDetachedSignature(keyring, digest, signature)
	if err != nil {
		return nil, err
	}
	if signer == nil {
		return nil, util.Errorf("Unsigned")
	}

	_, err = digest.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	return ParseDigest(digest)
}

// Parses a sha256sum digest. Each line is a 64-character sha256 hash in
// lowercase hexadecimal, followed by a space, followed by a space or
// asterisk (indicating text or binary), followed by a filename.
func ParseDigest(r io.Reader) (map[string]string, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)

	ret := map[string]string{}
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		if len(line) <= hashLength+2 {
			// this line could not have been produced by a sha1sum
			return ret, util.Errorf("%q is not valid shasum output", line)
		}

		filename := line[hashLength+2:]
		cleanname := filepath.Clean(filename)
		if _, ok := ret[cleanname]; ok {
			return ret, util.Errorf("Duplicate SHA for filename %q", filename)
		}
		ret[cleanname] = line[:hashLength]
	}
	return ret, nil
}
