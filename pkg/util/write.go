package util

import (
	"bytes"
	"io/ioutil"
	"os"
)

func WriteIfChanged(filename string, data []byte, perm os.FileMode) (bool, error) {
	modified := false

	content, err := ioutil.ReadFile(filename)
	if !os.IsNotExist(err) && err != nil {
		// file existed, but could not be read
		return modified, err
	}

	if os.IsNotExist(err) || bytes.Compare(content, data) != 0 {
		openPerm := perm
		if perm == 0 {
			// if the file gets created with perm=0, then all the permission
			// bits will be turned off
			// a more sensible default is to use 0666 (and then umask gets
			// applied)
			openPerm = 0666
		}

		modified = true
		err = ioutil.WriteFile(filename, data, openPerm)
		if err != nil {
			return modified, err
		}
	}

	if perm != 0 {
		info, err := os.Stat(filename)
		if err != nil {
			return modified, err
		}

		if info.Mode() != perm {
			modified = true
			err = os.Chmod(filename, perm)
		}
	}

	return modified, err
}
