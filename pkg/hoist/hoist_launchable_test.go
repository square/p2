package hoist

import (
	"io/ioutil"
	"net/url"
	"os"
	"os/user"
	"path"
	"runtime"
	"testing"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
	"gopkg.in/yaml.v2"

	. "github.com/anthonybishopric/gotcha"
)

func TestInstall(t *testing.T) {
	fetcher := uri.NewLoggedFetcher(nil)
	testContext := util.From(runtime.Caller(0))

	currentUser, err := user.Current()
	Assert(t).IsNil(err, "test setup: couldn't get current user")

	testLocation := &url.URL{
		Path: testContext.ExpandPath("hoisted-hello_def456.tar.gz"),
	}

	launchableHome, err := ioutil.TempDir("", "launchable_home")
	Assert(t).IsNil(err, "test setup: couldn't create temp dir")
	defer os.RemoveAll(launchableHome)

	launchable := &Launchable{
		Id:        "hello",
		Version:   "def456",
		ServiceId: "hoisted-hello__hello",
		RunAs:     currentUser.Username,
		PodEnvDir: launchableHome,
		RootDir:   launchableHome,
	}

	downloader := artifact.NewLocationDownloader(testLocation, fetcher, auth.NopVerifier())
	err = launchable.Install(downloader)
	Assert(t).IsNil(err, "there should not have been an error when installing")

	Assert(t).AreEqual(
		*fetcher.SrcUri,
		*testLocation,
		"The correct url wasn't set for the curl library",
	)

	hoistedHelloUnpacked := path.Join(launchableHome, "installs", "hello_def456")
	if info, err := os.Stat(hoistedHelloUnpacked); err != nil || !info.IsDir() {
		t.Fatalf("Expected %s to be the unpacked artifact location", hoistedHelloUnpacked)
	}
	helloLaunch := path.Join(hoistedHelloUnpacked, "bin", "launch")
	if info, err := os.Stat(helloLaunch); err != nil || info.IsDir() {
		t.Fatalf("Expected %s to be a the launch script for hello", helloLaunch)
	}
}

func TestInstallDir(t *testing.T) {
	tempDir := os.TempDir()

	launchable := &Launchable{
		Id:        "testLaunchable",
		Version:   "abc123",
		ServiceId: "testPod__testLaunchable",
		RunAs:     "testuser",
		PodEnvDir: tempDir,
		RootDir:   tempDir,
	}

	installDir := launchable.InstallDir()

	expectedDir := path.Join(tempDir, "installs", "testLaunchable_abc123")
	Assert(t).AreEqual(installDir, expectedDir, "Install dir did not have expected value")
}

func TestMultipleExecutables(t *testing.T) {
	fakeLaunchable, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(fakeLaunchable, sb)
	executables, err := fakeLaunchable.Executables(runit.DefaultBuilder)

	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1", "/var/service/testPod__testLaunchable__script2"}
	Assert(t).AreEqual(2, len(executables), "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Service.Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
	Assert(t).AreEqual(executables[1].Service.Path, expectedServicePaths[1], "Runit service paths from launchable did not match expected")
}

func TestSingleRunitService(t *testing.T) {
	launchable, sb := FakeHoistLaunchableForDir("single_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(launchable, sb)
	Assert(t).IsNil(launchable.MakeCurrent(), "Should have been made current")
	executables, err := launchable.Executables(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1"}
	Assert(t).AreEqual(len(executables), 1, "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Service.Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
}

func TestLaunchExecutableOnlyRunitService(t *testing.T) {
	launchable, sb := FakeHoistLaunchableForDir("launch_script_only_test_hoist_launchable")
	defer CleanupFakeLaunchable(launchable, sb)
	Assert(t).IsNil(launchable.MakeCurrent(), "Should have been made current")
	executables, err := launchable.Executables(runit.DefaultBuilder)
	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__launch"}
	Assert(t).AreEqual(len(executables), 1, "Found an unexpected number of runit services")
	Assert(t).AreEqual(executables[0].Service.Path, expectedServicePaths[0], "Runit service paths from launchable did not match expected")
}

func TestDisable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	disableOutput, err := hl.disable()
	Assert(t).IsNil(err, "Got an unexpected error when calling disable on the test hoist launchable")

	expectedDisableOutput := "disable invoked\n"

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

func TestFailingDisable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("failing_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	disableOutput, err := hl.disable()
	Assert(t).IsNotNil(err, "Expected disable to fail for this test, but it didn't")

	expectedDisableOutput := "Error: this script failed\n"

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

// providing a disable script is optional, make sure we don't error
func TestNonexistentDisable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("nonexistent_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	disableOutput, err := hl.disable()
	Assert(t).IsNil(err, "Got an unexpected error when calling disable on the test hoist launchable")

	expectedDisableOutput := ""

	Assert(t).AreEqual(disableOutput, expectedDisableOutput, "Did not get expected output from test disable script")
}

func TestEnable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	enableOutput, err := hl.enable()
	Assert(t).IsNil(err, "Got an unexpected error when calling enable on the test hoist launchable")

	expectedEnableOutput := "enable invoked\n"

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

func TestFailingEnable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("failing_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	enableOutput, err := hl.enable()
	Assert(t).IsNotNil(err, "Expected enable to fail for this test, but it didn't")

	expectedEnableOutput := "Error: this script failed\n"

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

// providing an enable script is optional, make sure we don't error
func TestNonexistentEnable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("nonexistent_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	enableOutput, err := hl.enable()
	Assert(t).IsNil(err, "Got an unexpected error when calling enable on the test hoist launchable")

	expectedEnableOutput := ""

	Assert(t).AreEqual(enableOutput, expectedEnableOutput, "Did not get expected output from test enable script")
}

func TestFailingStop(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.ErringSV()

	err := hl.stop(sb, sv)

	Assert(t).IsNotNil(err, "Expected sv stop to fail for this test, but it didn't")
}

func TestStart(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)
	sv := runit.FakeSV()
	executables, err := hl.Executables(sb)
	outFilePath := path.Join(sb.ConfigRoot, "testPod__testLaunchable.yaml")

	sbContentsMap := map[string]interface{}{
		executables[0].Service.Name: map[string]interface{}{
			"run": executables[0].Exec,
		},
		executables[1].Service.Name: map[string]interface{}{
			"run": executables[1].Exec,
		},
	}
	sbContents, err := yaml.Marshal(sbContentsMap)
	Assert(t).IsNil(err, "should have no error marshalling servicebuilder map")
	f, err := os.Open(outFilePath)
	defer f.Close()
	f.Write(sbContents)

	err = hl.start(sb, sv)

	Assert(t).IsNil(err, "Got an unexpected error when attempting to start runit services")

}

func TestFailingStart(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.ErringSV()
	executables, _ := hl.Executables(sb)
	outFilePath := path.Join(sb.ConfigRoot, "testPod__testLaunchable.yaml")

	sbContentsMap := map[string]interface{}{
		executables[0].Service.Name: map[string]interface{}{
			"run": executables[0].Exec,
		},
		executables[1].Service.Name: map[string]interface{}{
			"run": executables[1].Exec,
		},
	}
	sbContents, err := yaml.Marshal(sbContentsMap)
	Assert(t).IsNil(err, "should have no error marshalling servicebuilder map")
	f, err := os.Open(outFilePath)
	defer f.Close()
	f.Write(sbContents)

	err = hl.start(sb, sv)
	Assert(t).IsNotNil(err, "Expected an error starting runit services")
}

func TestStop(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.FakeSV()
	err := hl.stop(sb, sv)

	Assert(t).IsNil(err, "Got an unexpected error when attempting to stop runit services")
}

func TestHaltWithFailingDisable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("failing_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.FakeSV()
	err := hl.Halt(sb, sv)
	Assert(t).IsNotNil(err, "Expected error while halting")
	_, ok := err.(launch.DisableError)
	Assert(t).IsTrue(ok, "Expected disable error to be returned")
	_, ok = err.(launch.StopError)
	Assert(t).IsFalse(ok, "Did not expect stop error to be returned")
}

func TestHaltWithPassingDisable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.FakeSV()
	err := hl.Halt(sb, sv)
	Assert(t).IsNil(err, "Expected halt to succeed")

	Assert(t).IsNil(os.Remove(hl.LastDir()), "expected halt to create last symlink")
}

func TestLaunchWithFailingEnable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("failing_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.FakeSV()
	err := hl.Launch(sb, sv)
	Assert(t).IsNotNil(err, "Expected error while launching")
	_, ok := err.(launch.EnableError)
	Assert(t).IsTrue(ok, "Expected enable error to be returned")
	_, ok = err.(launch.StartError)
	Assert(t).IsFalse(ok, "Did not expect start error to be returned")
}

func TestLaunchWithPassingEnable(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.FakeSV()
	err := hl.Launch(sb, sv)
	Assert(t).IsNil(err, "Expected launch to succeed")
}

func TestHaltWithFailingStop(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.ErringSV()
	err := hl.Halt(sb, sv)
	Assert(t).IsNotNil(err, "Expected error while halting")
	_, ok := err.(launch.StopError)
	Assert(t).IsTrue(ok, "Expected stop error to be returned")
	_, ok = err.(launch.DisableError)
	Assert(t).IsFalse(ok, "Did not expect disable error to be returned")
}

func TestLaunchWithFailingStart(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.ErringSV()
	err := hl.Launch(sb, sv)
	Assert(t).IsNotNil(err, "Expected error while launching")
	_, ok := err.(launch.StartError)
	Assert(t).IsTrue(ok, "Expected start error to be returned")
	_, ok = err.(launch.EnableError)
	Assert(t).IsFalse(ok, "Did not expect enable error to be returned")
}

func TestOnceIfRestartNever(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	// If the launchable isn't intended to be restarted, the launchable
	// should launch using 'sv once' instead of 'sv restart'
	hl.RestartPolicy = runit.RestartPolicyNever

	sv := runit.NewRecordingSV()
	err := hl.Launch(sb, sv)
	Assert(t).IsNil(err, "Unexpected error when launching")
	commands := sv.(*runit.RecordingSV).Commands
	Assert(t).AreEqual(len(commands), 2, "expected 2 commands to be issued")
	Assert(t).AreEqual(commands[0], "once", "Expected 'once' command to be used for a launchable with RestartPolicyNever")
	Assert(t).AreEqual(commands[1], "restart", "Expected 'restart' command to be used for a the logAgent")
}

func TestRestartIfRestartAlways(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	// If the launchable is intended to be restarted, the launchable
	// should launch using 'sv restart'
	hl.RestartPolicy = runit.RestartPolicyAlways

	sv := runit.NewRecordingSV()
	err := hl.Launch(sb, sv)
	Assert(t).IsNil(err, "Unexpected error when launching")
	Assert(t).AreEqual(sv.(*runit.RecordingSV).LastCommand(), "restart", "Expected 'restart' command to be used for a launchable with RestartPolicyAlways")
}

func TestRestartServiceAndLogAgent(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)
	sv := runit.NewRecordingSV()

	hl.RestartPolicy = runit.RestartPolicyAlways
	hl.start(sb, sv)

	commands := sv.(*runit.RecordingSV).Commands
	Assert(t).AreEqual(len(commands), 2, "Expected 2 restart commands to be issued")
	Assert(t).AreEqual(commands[0], "restart", "Expected 2 restart commands to be issued")
	Assert(t).AreEqual(commands[1], "restart", "Expected 2 restart commands to be issued")
}
