package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"strconv"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func TestMkdirAll(t *testing.T) {
	temp, err := ioutil.TempDir("", "mkdirall")
	Assert(t).IsNil(err, "There should not have been an error creating a temp dir")

	dirPath := fmt.Sprintf("%s/foo/bar/baz", temp)

	curUser, err := user.Current()
	Assert(t).IsNil(err, "There should not have been an error checking the current user")
	uid, err := strconv.ParseInt(curUser.Uid, 10, 0)
	Assert(t).IsNil(err, "There should not have been an error converting uid to int")
	gid, err := strconv.ParseInt(curUser.Gid, 10, 0)
	Assert(t).IsNil(err, "There should not have been an error converting gid to int")

	err = MkdirChownAll(dirPath, int(uid), int(gid), 0777)
	Assert(t).IsNil(err, "There should not have been an error creating the directory")

	_, err = os.Stat(dirPath)
	Assert(t).IsNil(err, "There should not have been an error statting the directory")

	err = os.RemoveAll(temp)
	Assert(t).IsNil(err, "There should not have been an error cleaning up the temp directory")
}
