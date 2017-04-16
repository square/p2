package hoist

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
)

func helloTar() string {
	return util.From(runtime.Caller(0)).ExpandPath("hoisted-hello_def456.tar.gz")
}

func cleanupCacher(a *ArtifactCacher) {
	os.RemoveAll(a.cacheDir)
}

func tempCacher(t *testing.T) *ArtifactCacher {
	dir, err := ioutil.TempDir("", "cacher-test")
	Assert(t).IsNil(err, "Should not have erred creating a temp dir")
	cacher, err := WithCacher(DefaultFetcher(), dir)
	Assert(t).IsNil(err, "test setup error - could not create cacher")
	return cacher
}

func TestArtifactCacheWillCacheNewlyDiscoveredArtifacts(t *testing.T) {
	dest, err := ioutil.TempDir("", "download-target")
	Assert(t).IsNil(err, "Should not have erred creating a temp dir")
	defer os.RemoveAll(dest)
	cacher := tempCacher(t)
	defer cleanupCacher(cacher)
	helloDest := filepath.Join(dest, "hello.tar.gz")
	err = cacher.Fetch(helloTar(), helloDest)

	Assert(t).IsNil(err, "Cacher returned an error when fetching")

	info, err := os.Stat(filepath.Join(cacher.cacheDir, "hello.tar.gz"))
	Assert(t).IsNil(err, "should not have erred stating the expected cached tar location")
	Assert(t).IsTrue(info.Size() > 0, "should have cached something")

	info, err = os.Stat(helloDest)
	Assert(t).IsNil(err, "should not have erred stating the expected destination")
	Assert(t).IsTrue(info.Size() > 0, "should have actually fetched to the target")
}

func TestArtifactCacheWillTouchExistingArtifactsOnAccess(t *testing.T) {
	cacher := tempCacher(t)
	defer cleanupCacher(cacher)
	preCopiedPath := filepath.Join(cacher.cacheDir, filepath.Base(helloTar()))
	err := uri.URICopy(helloTar(), preCopiedPath)
	Assert(t).IsNil(err, "test setup - couldn't prewarm cache")
	aYearAgo := time.Now().Add(-365 * 24 * time.Hour)
	anHourAgo := time.Now().Add(-time.Hour)

	fmt.Println(aYearAgo)
	fmt.Println(anHourAgo)

	err = os.Chtimes(preCopiedPath, aYearAgo, aYearAgo)
	Assert(t).IsNil(err, "test setup - should have been able to change mtime")

	dest, err := ioutil.TempDir("", "download-target")
	Assert(t).IsNil(err, "test setup - should not have erred creating a temp dir")
	defer os.RemoveAll(dest)

	err = cacher.Fetch(helloTar(), filepath.Join(dest, filepath.Base(helloTar())))
	Assert(t).IsNil(err, "cacher erred during copy")

	info, err := os.Stat(preCopiedPath)
	Assert(t).IsNil(err, "should not have erred stating the cache path")

	fmt.Println(info.ModTime())
	Assert(t).IsTrue(info.ModTime().After(anHourAgo), "cacher didn't touch the pre-copied path")
}

func TestArtifactCacheWillPruneAllIfKeepIsZero(t *testing.T) {
	dest, err := ioutil.TempDir("", "download-target")
	Assert(t).IsNil(err, "Should not have erred creating a temp dir")
	defer os.RemoveAll(dest)
	cacher := tempCacher(t)
	defer cleanupCacher(cacher)
	helloDest := filepath.Join(dest, "hello.tar.gz")
	err = cacher.Fetch(helloTar(), helloDest)
	helloDest2 := filepath.Join(dest, "hello2.tar.gz")
	err = cacher.Fetch(helloTar(), helloDest2)
	helloDest3 := filepath.Join(dest, "hello3.tar.gz")
	err = cacher.Fetch(helloTar(), helloDest3)

	cacher.Prune(1)

	files, err := ioutil.ReadDir(cacher.cacheDir)

	Assert(t).IsNil(err, "Should not have erred reading dir")
	Assert(t).AreEqual(1, len(files), "Should have had 1 file left")

	cacher.Prune(0)

	files, err = ioutil.ReadDir(cacher.cacheDir)

	Assert(t).IsNil(err, "Should not have erred reading dir")
	Assert(t).AreEqual(0, len(files), "Should have had no files left")
}
