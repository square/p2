package hoist

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/square/p2/pkg/util/size"

	. "github.com/anthonybishopric/gotcha"
)

type testInstall struct {
	name      string
	modTime   time.Time
	byteCount size.ByteCount
}

func (i testInstall) create(parent string) error {
	path := filepath.Join(parent, "installs", i.name)
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return err
	}
	payloadPath := filepath.Join(parent, "installs", i.name, "payload")
	file, err := os.OpenFile(payloadPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	var soFar int64
	bytesToWrite := []byte{'a'}
	for size.ByteCount(soFar) < i.byteCount {
		written, err := file.Write(bytesToWrite)
		if err != nil {
			return err
		}
		soFar += int64(written)
	}

	err = file.Close()
	if err != nil {
		return err
	}

	if i.name == "current" {
		err := os.Symlink(path, filepath.Join(parent, "current"))
		if err != nil {
			return err
		}
	}

	if i.name == "last" {
		err := os.Symlink(path, filepath.Join(parent, "last"))
		if err != nil {
			return err
		}
	}

	return os.Chtimes(path, i.modTime, i.modTime)
}

func launchableWithInstallations(t *testing.T, installs []testInstall, testFn func(*Launchable)) {
	parent, err := ioutil.TempDir("", "prune")
	defer os.RemoveAll(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range installs {
		err := i.create(parent)
		if err != nil {
			t.Fatal(err)
		}
	}
	launchable := &Launchable{
		RootDir: parent,
	}
	testFn(launchable)
}

func assertShouldBePruned(t *testing.T, hl *Launchable, name string) {
	_, err := os.Stat(filepath.Join(hl.AllInstallsDir(), name))
	Assert(t).IsTrue(os.IsNotExist(err), fmt.Sprintf("Should have removed %v", name))
}

func assertShouldExist(t *testing.T, hl *Launchable, name string) {
	info, err := os.Stat(filepath.Join(hl.AllInstallsDir(), name))
	Assert(t).IsNil(err, fmt.Sprintf("should not have erred stat'ing %v", name))
	Assert(t).IsTrue(info.IsDir(), fmt.Sprintf("Should not have removed the %v directory", name))
}

func TestPruneRemovesOldInstallations(t *testing.T) {
	launchableWithInstallations(t, []testInstall{
		{"first", time.Now().Add(-1000 * time.Hour), 10 * size.Kibibyte},
		{"second", time.Now().Add(-800 * time.Hour), 10 * size.Kibibyte},
		{"third", time.Now().Add(-600 * time.Hour), 10 * size.Kibibyte},
	}, func(hl *Launchable) {

		Assert(t).IsNil(hl.Prune(20*size.Kibibyte), "Should not have erred when pruning")

		assertShouldBePruned(t, hl, "first")
		assertShouldExist(t, hl, "second")
		assertShouldExist(t, hl, "third")
	})
}

func TestPruneIgnoresInstallsWhenUnderLimit(t *testing.T) {
	launchableWithInstallations(t, []testInstall{
		{"first", time.Now().Add(-1000 * time.Hour), 10 * size.Kibibyte},
		{"second", time.Now().Add(-800 * time.Hour), 10 * size.Kibibyte},
		{"third", time.Now().Add(-600 * time.Hour), 10 * size.Kibibyte},
	}, func(hl *Launchable) {
		Assert(t).IsNil(hl.Prune(2*size.Mebibyte), "Should not have erred when pruning")

		assertShouldExist(t, hl, "first")
		assertShouldExist(t, hl, "second")
		assertShouldExist(t, hl, "third")
	})
}

func TestPruneIgnoresCurrent(t *testing.T) {
	launchableWithInstallations(t, []testInstall{
		{"current", time.Now().Add(-1000 * time.Hour), 10 * size.Kibibyte},
		{"second", time.Now().Add(-800 * time.Hour), 10 * size.Kibibyte},
		{"third", time.Now().Add(-600 * time.Hour), 10 * size.Kibibyte},
	}, func(hl *Launchable) {
		Assert(t).IsNil(hl.Prune(20*size.Kibibyte), "Should not have erred when pruning")

		assertShouldExist(t, hl, "current")
		assertShouldBePruned(t, hl, "second")
		assertShouldExist(t, hl, "third")
	})
}

func TestPruneIgnoresCurrentAndLast(t *testing.T) {
	launchableWithInstallations(t, []testInstall{
		{"current", time.Now().Add(-1000 * time.Hour), 10 * size.Kibibyte},
		{"last", time.Now().Add(-800 * time.Hour), 10 * size.Kibibyte},
		{"third", time.Now().Add(-600 * time.Hour), 10 * size.Kibibyte},
	}, func(hl *Launchable) {
		Assert(t).IsNil(hl.Prune(20*size.Kibibyte), "Should not have erred when pruning")

		assertShouldExist(t, hl, "current")
		assertShouldExist(t, hl, "last")
		assertShouldBePruned(t, hl, "third")
	})
}
