package main

import (
	"archive/tar"
	"compress/gzip"
	"io/ioutil"
	"net/url"
	"os"
	"path"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Handling artifacts", func() {
	It("Extracts artifacts to install directory", func() {
		basedir := tempDir()
		defer os.RemoveAll(basedir)

		repo := tempDir()
		defer os.RemoveAll(repo)

		preparerConfig := PreparerConfig{
			ArtifactRepo: url.URL{Scheme: "file", Path: repo},
		}
		artifactSourcePath := path.Join(repo, "testapp", "testapp_abc123.tar.gz")

		writeTarGz(artifactSourcePath, map[string]string{
			"README": "Hello",
		})

		PrepareArtifact("testapp", "abc123", basedir, preparerConfig)
		expectedFile := path.Join(basedir, "installs", "testapp_abc123", "README")

		contents, err := ioutil.ReadFile(expectedFile)

		Expect(err).To(BeNil())
		Expect(string(contents)).To(Equal("Hello"))
	})

	It("Does not override existing install", func() {
		basedir := tempDir()
		defer os.RemoveAll(basedir)

		keepFile := path.Join(basedir, "installs/testapp_abc123/keep")
		os.MkdirAll(keepFile, 0755)

		PrepareArtifact("testapp", "abc123", basedir, PreparerConfig{})

		if _, err := os.Stat(keepFile); os.IsNotExist(err) {
			Fail("Overwrote existing file")
		}
	})
})

func tempDir() string {
	dir, err := ioutil.TempDir("", "preparer-test")
	if err != nil {
		panic(err)
	}
	return dir
}
func writeTarGz(dest string, files map[string]string) {
	os.MkdirAll(path.Dir(dest), 0755)

	f, _ := os.Create(dest)
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	for name, content := range files {
		hdr := &tar.Header{
			Name: name,
			Size: int64(len(content)),
			Mode: 0644,
		}
		tw.WriteHeader(hdr)
		tw.Write([]byte(content))
	}
}
