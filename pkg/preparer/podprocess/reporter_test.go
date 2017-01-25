package podprocess

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/statusstore/podstatus"
	"github.com/square/p2/pkg/store/consul/statusstore/statusstoretest"
	"github.com/square/p2/pkg/types"
)

func TestFullyConfigured(t *testing.T) {
	ok := ReporterConfig{}.FullyConfigured()
	if ok {
		t.Error("Should have gotten false for reporter with empty config")
	}

	ok = ReporterConfig{
		SQLiteDatabasePath: "foo",
	}.FullyConfigured()
	if ok {
		t.Errorf("Should have gotten false with config with missing environemnt extractor path")
	}

	ok = ReporterConfig{
		EnvironmentExtractorPath: "foo",
	}.FullyConfigured()
	if ok {
		t.Errorf("Should have gotten false with missing database path")
	}

	ok = ReporterConfig{
		SQLiteDatabasePath:       "foo",
		EnvironmentExtractorPath: "bar",
		WorkspaceDirPath:         "some/dir",
	}.FullyConfigured()
	if !ok {
		t.Errorf("Should have gotten true when all config is specified")
	}
}

func TestNew(t *testing.T) {
	reporter, err := New(ReporterConfig{}, logging.DefaultLogger, podstatus.NewConsul(statusstoretest.NewFake(), consul.PreparerPodStatusNamespace))
	if reporter != nil || err == nil {
		t.Errorf("Should have gotten a nil reporter and an error with empty config")
	}

	reporter, err = New(ReporterConfig{
		SQLiteDatabasePath:       "bar",
		EnvironmentExtractorPath: "/some/nonexistent/path",
	}, logging.DefaultLogger, podstatus.NewConsul(statusstoretest.NewFake(), consul.PreparerPodStatusNamespace))
	if reporter != nil || err == nil {
		t.Errorf("Should have gotten a nil reporter when EnvironmentExtractorPath doesn't exist")
	}

	tempDir, err := ioutil.TempDir("", "podprocess_reporter_tests")
	if err != nil {
		t.Fatalf("Could not set up temp directory for tests: %s", err)
	}
	defer os.RemoveAll(tempDir)

	// os.Create() creates a file with mode 0666, which is not executable
	nonExecutableExtractor, err := os.Create(filepath.Join(tempDir, "non_executable_extractor"))
	if err != nil {
		t.Fatalf("Could not create non-executable extractor file")
	}

	reporter, err = New(ReporterConfig{
		SQLiteDatabasePath:       "foo",
		EnvironmentExtractorPath: nonExecutableExtractor.Name(),
	}, logging.DefaultLogger, podstatus.NewConsul(statusstoretest.NewFake(), consul.PreparerPodStatusNamespace))
	if reporter != nil || err == nil {
		t.Errorf("Should have gotten a nil reporter with non-executable environemnt_extractor_path")
	}

	// now make it executable
	executableExtractor := nonExecutableExtractor
	err = executableExtractor.Chmod(0777)
	if err != nil {
		t.Fatalf("Could not make environment extractor path executable: %s", err)
	}

	reporter, err = New(ReporterConfig{
		SQLiteDatabasePath:       "foo",
		EnvironmentExtractorPath: executableExtractor.Name(),
	}, logging.DefaultLogger, podstatus.NewConsul(statusstoretest.NewFake(), consul.PreparerPodStatusNamespace))
	if err != nil {
		t.Errorf("Unexpected error calling New(): %s", err)
	}
}

func TestRun(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "process_reporter")
	if err != nil {
		t.Fatalf("Could not create temp dir: %s", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath, quitCh, podStatusStore, _ := startReporter(t, tempDir)
	defer close(quitCh)

	finishOutput1 := FinishOutput{
		PodID:        "some_pod",
		LaunchableID: "some_launchable",
		EntryPoint:   "launch",
		PodUniqueKey: types.NewPodUUID(),
		ExitCode:     1,
		ExitStatus:   120,
	}

	finishService, err := NewSQLiteFinishService(dbPath, logging.DefaultLogger)
	if err != nil {
		t.Fatalf("Could not initialize finish service: %s", err)
	}
	defer finishService.Close()

	err = finishService.Insert(finishOutput1)
	if err != nil {
		t.Fatalf("Could not insert first finish value into the database: %s", err)
	}

	assertStatusUpdated(t, finishOutput1, podStatusStore)

	finishOutput2 := FinishOutput{
		PodID:        "some_pod",
		LaunchableID: "some_launchable",
		EntryPoint:   "nginx_worker",
		PodUniqueKey: types.NewPodUUID(),
		ExitCode:     3,
		ExitStatus:   67,
	}
	err = finishService.Insert(finishOutput2)
	if err != nil {
		t.Fatalf("Could not insert second finish value into the database: %s", err)
	}

	assertStatusUpdated(t, finishOutput2, podStatusStore)
}

// The reporter operates by processing events from sqlite and then writing the
// highest handled ID to a file for use in the next iteration to avoid
// double-submitting a pod process finish. This test confirms that the reporter
// can recover from a problem with the file, such as it containing 0 bytes or
// having non-integer contents
func TestRepairCorruptWorkspaceFile(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "process_reporter")
	if err != nil {
		t.Fatalf("Could not create temp dir: %s", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath, quitCh, podStatusStore, reporter := startReporter(t, tempDir)
	defer close(quitCh)

	finishOutput1 := FinishOutput{
		PodID:        "some_pod",
		LaunchableID: "some_launchable",
		EntryPoint:   "launch",
		PodUniqueKey: types.NewPodUUID(),
		ExitCode:     1,
		ExitStatus:   120,
	}

	finishService, err := NewSQLiteFinishService(dbPath, logging.DefaultLogger)
	if err != nil {
		t.Fatalf("Could not initialize finish service: %s", err)
	}
	defer finishService.Close()

	err = finishService.Insert(finishOutput1)
	if err != nil {
		t.Fatalf("Could not insert first finish value into the database: %s", err)
	}

	assertStatusUpdated(t, finishOutput1, podStatusStore)

	// now truncate the workspace file and ensure that the file is repaired
	err = os.Truncate(reporter.workspaceFilePath(), 0)
	if err != nil {
		t.Fatalf("could not truncate workspace file to test for repair: %s", err)
	}

	for try := 0; try < 10; try++ {
		lastID, err := reporter.getLastID()
		if err == nil {
			if lastID == 1 {
				// the file was repaired
				break
			}
		} else {
			time.Sleep(1 * time.Millisecond)
			// we ignore errors because we expect them, what we really want
			// is that eventually there's no error and 1 is returned
		}
	}

	finishOutput2 := FinishOutput{
		PodID:        "some_pod",
		LaunchableID: "some_launchable",
		EntryPoint:   "nginx_worker",
		PodUniqueKey: types.NewPodUUID(),
		ExitCode:     3,
		ExitStatus:   67,
	}
	err = finishService.Insert(finishOutput2)
	if err != nil {
		t.Fatalf("Could not insert second finish value into the database: %s", err)
	}

	assertStatusUpdated(t, finishOutput2, podStatusStore)
}

func assertStatusUpdated(t *testing.T, finish FinishOutput, podStatusStore podstatus.Store) {
	statusRetrieved := false
	var status podstatus.PodStatus
	var err error
	for i := 0; i < 100; i++ {
		status, _, err = podStatusStore.Get(finish.PodUniqueKey)
		if err == nil {
			statusRetrieved = true
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !statusRetrieved {
		t.Fatalf("Could not retrieve status for the pod %s__%s__%s: %s", finish.PodID, finish.LaunchableID, finish.EntryPoint, err)
	}

	found := false
	for _, processStatus := range status.ProcessStatuses {
		if processStatus.LaunchableID == finish.LaunchableID && processStatus.EntryPoint == finish.EntryPoint {
			found = true
			if processStatus.LastExit.ExitStatus != finish.ExitStatus {
				t.Errorf(
					"Expected exit status of %d but got %d for pod %s__%s__%s",
					finish.ExitStatus,
					processStatus.LastExit.ExitStatus,
					finish.PodID,
					finish.LaunchableID,
					finish.EntryPoint,
				)
			}

			if processStatus.LastExit.ExitCode != finish.ExitCode {
				t.Errorf(
					"Expected exit code of %d but got %d for pod %s__%s__%s",
					finish.ExitCode,
					processStatus.LastExit.ExitCode,
					finish.PodID,
					finish.LaunchableID,
					finish.EntryPoint,
				)
			}
		}
	}

	if !found {
		t.Error("Did not find the first exited process in the status store")
	}
}

func startReporter(t *testing.T, tempDir string) (string, chan struct{}, podstatus.Store, *Reporter) {
	dbPath := filepath.Join(tempDir, "finishes.db")

	// The extractor doesn't need to do anything because we'll mimic its behavior in the test,
	// but it needs to exist and be executable in order to pass constructor validation
	extractor, err := os.Create(filepath.Join(tempDir, "env_extractor"))
	if err != nil {
		t.Fatalf("Could not create env extractor file: %s", err)
	}
	// make it executable
	err = extractor.Chmod(0777)
	if err != nil {
		t.Fatalf("could not make env extractor executable: %s", err)
	}
	config := ReporterConfig{
		SQLiteDatabasePath: dbPath,
		// This setting only matters for generating the FinishExec in the preparer,
		// it won't affect the tests in this file
		EnvironmentExtractorPath: extractor.Name(),
		WorkspaceDirPath:         tempDir,
		PollInterval:             1 * time.Millisecond,
	}

	store := podstatus.NewConsul(statusstoretest.NewFake(), consul.PreparerPodStatusNamespace)
	reporter, err := New(config, logging.DefaultLogger, store)
	if err != nil {
		t.Fatalf("Error creating reporter: %s", err)
	}
	quitCh := make(chan struct{})

	err = reporter.Run(quitCh)
	if err != nil {
		t.Fatalf("Unable to start reporter: %s", err)
	}
	return dbPath, quitCh, store, reporter
}
