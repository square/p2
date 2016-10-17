package hoist

import (
	"os"
	"path"
	"testing"

	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/runit"
	"gopkg.in/yaml.v2"

	. "github.com/anthonybishopric/gotcha"
)

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

func TestInstallDirNoVersion(t *testing.T) {
	tempDir := os.TempDir()

	launchable := &Launchable{
		Id:        "testLaunchable",
		Version:   "",
		ServiceId: "testPod__testLaunchable",
		RunAs:     "testuser",
		PodEnvDir: tempDir,
		RootDir:   tempDir,
	}

	installDir := launchable.InstallDir()

	expectedDir := path.Join(tempDir, "installs", "testLaunchable")
	Assert(t).AreEqual(installDir, expectedDir, "Install dir did not have expected value")
}

// This test is plays out the scenario where no entry points are explicitly declared
// in the launchable stanza for the launchable. Therefore, bin/launch should be treated
// as an entry point and that is it.
func TestMultipleExecutables(t *testing.T) {
	fakeLaunchable, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(fakeLaunchable, sb)
	executables, err := fakeLaunchable.Executables(runit.DefaultBuilder)

	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{"/var/service/testPod__testLaunchable__script1", "/var/service/testPod__testLaunchable__script2"}
	Assert(t).AreEqual(2, len(executables), "Found an unexpected number of runit services")

	for _, expected := range expectedServicePaths {
		found := false
		for _, executable := range executables {
			if executable.Service.Path == expected {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Did not find %s among executable paths", expected)
		}
	}
}

// This test exercises the functionality of having two entry points specified in a pod manifest,
// in this case bin/launch and bin/start
func TestMultipleEntryPoints(t *testing.T) {
	fakeLaunchable, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(fakeLaunchable, sb)

	fakeLaunchable.EntryPoints = append(fakeLaunchable.EntryPoints, "bin/start")
	executables, err := fakeLaunchable.Executables(runit.DefaultBuilder)

	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{
		"/var/service/testPod__testLaunchable__script1",
		"/var/service/testPod__testLaunchable__script2",
		"/var/service/testPod__testLaunchable__start",
	}
	Assert(t).AreEqual(3, len(executables), "Found an unexpected number of runit services")

	for _, expected := range expectedServicePaths {
		found := false
		for _, executable := range executables {
			if executable.Service.Path == expected {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Did not find %s among executable paths", expected)
		}
	}
}

// Tests that bin/launch is ignored if it is not specified as an entry point
func TestNonStandardEntryPoint(t *testing.T) {
	fakeLaunchable, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(fakeLaunchable, sb)

	fakeLaunchable.EntryPoints = []string{"bin/start"}
	executables, err := fakeLaunchable.Executables(runit.DefaultBuilder)

	Assert(t).IsNil(err, "Error occurred when obtaining runit services for launchable")

	expectedServicePaths := []string{
		"/var/service/testPod__testLaunchable__start",
	}
	Assert(t).AreEqual(1, len(executables), "Found an unexpected number of runit services")

	for _, expected := range expectedServicePaths {
		found := false
		for _, executable := range executables {
			if executable.Service.Path == expected {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Did not find %s among executable paths", expected)
		}
	}
}

// Tests that the Executables() function fails if two runit services with the same
// service name will be created. We specify bin/start (a normal file) and bin/start2
// (a directory containing one file called start) which will generate a naming
// collision and thus (we hope) an error
func TestErrorIfConflictingServiceNames(t *testing.T) {
	fakeLaunchable, sb := FakeHoistLaunchableForDir("multiple_script_test_hoist_launchable")
	defer CleanupFakeLaunchable(fakeLaunchable, sb)

	fakeLaunchable.EntryPoints = []string{"bin/start", "bin/start2"}
	executables, err := fakeLaunchable.Executables(runit.DefaultBuilder)

	Assert(t).IsNotNil(err, "Expected naming collision error calling Executables()")
	Assert(t).AreEqual(0, len(executables), "Found an unexpected number of runit services")
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

func TestDisableWithFailingDisable(t *testing.T) {
	hl, _ := FakeHoistLaunchableForDir("failing_scripts_test_hoist_launchable")
	err := hl.Disable()
	Assert(t).IsNotNil(err, "Expected error while disabling")
	_, ok := err.(launch.DisableError)
	Assert(t).IsTrue(ok, "Expected disable error to be returned")
}

func TestDisableWithPassingDisable(t *testing.T) {
	hl, _ := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	err := hl.Disable()
	Assert(t).IsNil(err, "Expected disable to succeed")
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

func TestStopWithFailure(t *testing.T) {
	hl, sb := FakeHoistLaunchableForDir("successful_scripts_test_hoist_launchable")
	defer CleanupFakeLaunchable(hl, sb)

	sv := runit.ErringSV()
	err := hl.Stop(sb, sv)
	Assert(t).IsNotNil(err, "Expected error while halting")
	_, ok := err.(launch.StopError)
	Assert(t).IsTrue(ok, "Expected stop error to be returned")
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
