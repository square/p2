package pods

import (
	"bytes"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"path"
	"strings"
	"testing"

	. "github.com/anthonybishopric/gotcha"
)

func TestExecutableWritesValidScript(t *testing.T) {
	user, err := user.Current()
	Assert(t).IsNil(err, "test setup failure - should not have failed to get current user")
	envdir, err := ioutil.TempDir("", "envdir")
	scriptdir, err := ioutil.TempDir("", "scriptdir")
	defer os.RemoveAll(envdir)
	defer os.RemoveAll(scriptdir)
	Assert(t).IsNil(err, "test setup failure - should not have failed to get a temp dir")
	err = ioutil.WriteFile(path.Join(envdir, "SPECIALTESTVAR"), []byte("specialvalue"), 0644)
	Assert(t).IsNil(err, "test setup failure - should not have failed to write an environment var")
	executable := &HoistExecutable{
		Chpst:     FakeChpst(),
		Nolimit:   "",
		ExecPath:  "/usr/bin/env",
		RunAs:     user.Username,
		ConfigDir: envdir,
	}
	scriptPath := path.Join(scriptdir, "script")
	scriptHandle, err := os.OpenFile(scriptPath, os.O_CREATE|os.O_WRONLY, 0744)
	Assert(t).IsNil(err, "test setup failure - could not open test script for writing")

	Assert(t).IsNil(executable.WriteExecutor(scriptHandle), "could not write executable file")
	Assert(t).IsNil(scriptHandle.Close(), "could not close file handle for the script")

	cmd := exec.Command(scriptPath)
	out := &bytes.Buffer{}
	cmd.Stdout = out
	cmd.Stderr = os.Stderr
	Assert(t).IsNil(cmd.Run(), "should not have erred when running the script")

	Assert(t).IsTrue(strings.Contains(out.String(), "SPECIALTESTVAR=specialvalue"), "the output of the script should have contained the environment variable that was exported to the config dir")
}
