package digest

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func TestVerifySimpleTree(t *testing.T) {
	workdir, err := ioutil.TempDir("", "verification")
	Assert(t).IsNil(err, "temp dir error should be nil")
	defer os.RemoveAll(workdir)

	err = ioutil.WriteFile(filepath.Join(workdir, "a"), []byte("a"), 0644)
	Assert(t).IsNil(err, "temp file error should be nil")
	err = ioutil.WriteFile(filepath.Join(workdir, "b"), []byte("b"), 0644)
	Assert(t).IsNil(err, "temp file error should be nil")
	err = os.Mkdir(filepath.Join(workdir, "c"), 0755)
	Assert(t).IsNil(err, "temp dir error should be nil")
	err = ioutil.WriteFile(filepath.Join(workdir, "c", "d"), []byte("d"), 0644)
	Assert(t).IsNil(err, "temp file error should be nil")

	digest := map[string]string{
		"a":   "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
		"b":   "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d",
		"c/d": "18ac3e7343f016890c510e93f935261169d9e3f565436429830faf0934f4f8e4",
	}

	Assert(t).IsNil(VerifyDir(workdir, digest), "tree verification should have succeeded")
}

func TestVerifyFailsIfManifestMissingFile(t *testing.T) {
	workdir, err := ioutil.TempDir("", "verification")
	Assert(t).IsNil(err, "temp dir error should be nil")
	defer os.RemoveAll(workdir)

	err = ioutil.WriteFile(filepath.Join(workdir, "a"), []byte("a"), 0644)
	Assert(t).IsNil(err, "temp file error should be nil")
	err = ioutil.WriteFile(filepath.Join(workdir, "b"), []byte("b"), 0644)
	Assert(t).IsNil(err, "temp file error should be nil")
	err = os.Mkdir(filepath.Join(workdir, "c"), 0755)
	Assert(t).IsNil(err, "temp dir error should be nil")
	err = ioutil.WriteFile(filepath.Join(workdir, "c", "d"), []byte("d"), 0644)
	Assert(t).IsNil(err, "temp file error should be nil")

	digest := map[string]string{
		"a": "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
		"b": "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d",
	}

	Assert(t).IsNotNil(VerifyDir(workdir, digest), "tree verification should have failed")
}

func TestVerifyFailsIfTreeMissingFile(t *testing.T) {
	workdir, err := ioutil.TempDir("", "verification")
	Assert(t).IsNil(err, "temp dir error should be nil")
	defer os.RemoveAll(workdir)

	err = ioutil.WriteFile(filepath.Join(workdir, "a"), []byte("a"), 0644)
	Assert(t).IsNil(err, "temp file error should be nil")
	err = ioutil.WriteFile(filepath.Join(workdir, "b"), []byte("b"), 0644)
	Assert(t).IsNil(err, "temp file error should be nil")
	err = os.Mkdir(filepath.Join(workdir, "c"), 0755)
	Assert(t).IsNil(err, "temp dir error should be nil")

	digest := map[string]string{
		"a":   "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
		"b":   "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d",
		"c/d": "18ac3e7343f016890c510e93f935261169d9e3f565436429830faf0934f4f8e4",
	}

	Assert(t).IsNotNil(VerifyDir(workdir, digest), "tree verification should have failed")
}

func TestSimpleDigestParsing(t *testing.T) {
	digest := `ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb  ./a
3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d  ./b
18ac3e7343f016890c510e93f935261169d9e3f565436429830faf0934f4f8e4  ./c/d`

	parsedDigest, err := Parse(strings.NewReader(digest), nil)
	Assert(t).IsNil(err, "digest should have parsed")
	Assert(t).IsTrue(reflect.DeepEqual(parsedDigest.FileHashes, map[string]string{
		"a":   "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
		"b":   "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d",
		"c/d": "18ac3e7343f016890c510e93f935261169d9e3f565436429830faf0934f4f8e4",
	}), "manifests should have matched")
}
