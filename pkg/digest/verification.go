package digest

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/square/p2/pkg/util"
)

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
