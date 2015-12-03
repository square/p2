package hoist

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/square/p2/pkg/util/size"
)

// Prune will attempt to get the total size of the launchable's installs
// directory to be equal to or less than the maximum specified number of
// bytes. This method will preserve any directories that are pointed to
// by the `current` or `last` symlinks. Installations will be removed from
// oldest to newest.
func (hl *Launchable) Prune(maxSize size.ByteCount) error {
	curTarget, err := os.Readlink(hl.CurrentDir())
	if os.IsNotExist(err) {
		curTarget = ""
	} else if err != nil {
		return err
	}
	curTarget = filepath.Base(curTarget)

	lastTarget, err := os.Readlink(hl.LastDir())
	if os.IsNotExist(err) {
		lastTarget = ""
	} else if err != nil {
		return err
	}
	lastTarget = filepath.Base(lastTarget)

	installs, err := ioutil.ReadDir(hl.AllInstallsDir())
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	var totalSize size.ByteCount = 0

	installSizes := map[string]size.ByteCount{}

	for _, i := range installs {
		installSize, err := hl.sizeOfInstall(i.Name())
		if err != nil {
			return err
		}
		totalSize += size.ByteCount(installSize)
		installSizes[i.Name()] = installSize
	}

	oldestFirst := installsByAge(installs)
	sort.Sort(oldestFirst)

	for _, i := range oldestFirst {
		if totalSize <= maxSize {
			return nil
		}

		if i.Name() == curTarget || i.Name() == lastTarget {
			continue
		}

		installSize := installSizes[i.Name()]
		err = os.RemoveAll(filepath.Join(hl.AllInstallsDir(), i.Name()))
		if err != nil {
			return err
		}
		totalSize = totalSize - size.ByteCount(installSize)
	}

	return nil
}

func (hl *Launchable) sizeOfInstall(name string) (size.ByteCount, error) {
	var total int64
	err := filepath.Walk(filepath.Join(hl.AllInstallsDir(), name), func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			total += info.Size()
		}
		return err
	})
	return size.ByteCount(total), err
}

type installsByAge []os.FileInfo

func (in installsByAge) Less(i, j int) bool {
	return in[i].ModTime().Unix() < in[j].ModTime().Unix()
}

func (in installsByAge) Swap(i, j int) {
	in[i], in[j] = in[j], in[i]
}

func (in installsByAge) Len() int {
	return len(in)
}
