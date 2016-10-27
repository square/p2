package podprocess

import (
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/statusstore/podstatus"
	"github.com/square/p2/pkg/kp/statusstore/statusstoretest"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/types"
)

func TestNew(t *testing.T) {
	reporter, ok := New(ReporterConfig{}, logging.DefaultLogger, podstatus.NewConsul(statusstoretest.NewFake(), kp.PreparerPodStatusNamespace))
	if reporter != nil || ok {
		t.Errorf("Should have gotten a nil reporter with empty config")
	}

	reporter, ok = New(ReporterConfig{
		ProcessResultSocketPath: "foo",
	}, logging.DefaultLogger, podstatus.NewConsul(statusstoretest.NewFake(), kp.PreparerPodStatusNamespace))
	if reporter != nil || ok {
		t.Errorf("Should have gotten a nil reporter with missing EnvironmentExtractorPath")
	}

	reporter, ok = New(ReporterConfig{
		EnvironmentExtractorPath: "foo",
	}, logging.DefaultLogger, podstatus.NewConsul(statusstoretest.NewFake(), kp.PreparerPodStatusNamespace))
	if reporter != nil || ok {
		t.Errorf("Should have gotten a nil reporter with missing socket path")
	}

	reporter, ok = New(ReporterConfig{
		ProcessResultSocketPath:  "foo",
		EnvironmentExtractorPath: "bar",
	}, logging.DefaultLogger, podstatus.NewConsul(statusstoretest.NewFake(), kp.PreparerPodStatusNamespace))
	if reporter == nil || !ok {
		t.Errorf("Should have gotten a non-nil reporter when all config is specified")
	}
}

func TestRun(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "process_reporter")
	if err != nil {
		t.Fatalf("Could not create temp dir: %s", err)
	}
	defer os.RemoveAll(tempDir)

	socketPath, quitCh, podStatusStore := startReporter(t, tempDir)
	defer close(quitCh)

	validFinishOutput1 := FinishOutput{
		PodID:        "some_pod",
		LaunchableID: "some_launchable",
		EntryPoint:   "launch",
		PodUniqueKey: types.NewPodUUID(),
		ExitCode:     1,
		ExitStatus:   120,
	}
	bytes1, err := json.Marshal(validFinishOutput1)
	if err != nil {
		t.Fatalf("Could not marshal finish output as bytes: %s", err)
	}

	validFinishOutput2 := FinishOutput{
		PodID:        "some_pod",
		LaunchableID: "some_launchable",
		EntryPoint:   "nginx_worker",
		PodUniqueKey: types.NewPodUUID(),
		ExitCode:     3,
		ExitStatus:   67,
	}
	bytes2, err := json.Marshal(validFinishOutput2)
	if err != nil {
		t.Fatalf("Could not marshal finish output as bytes: %s", err)
	}

	messages := [][]byte{
		bytes1,       // valid
		[]byte{},     // invalid message
		[]byte("{}"), // parses but no PodUniqueKey
		bytes2,       // valid
	}

	var wg sync.WaitGroup
	for _, message := range messages {
		wg.Add(1)
		go func(msg []byte) {
			defer wg.Done()
			timeoutWriteMessage(t, socketPath, msg)
		}(message)
	}

	wg.Wait()

	// Now verify that the exit statuses made it to the pod status store
	for i, finish := range []FinishOutput{validFinishOutput1, validFinishOutput2} {
		statusRetrieved := false
		var status podstatus.PodStatus
		for i := 0; i < 3; i++ {
			status, _, err = podStatusStore.Get(finish.PodUniqueKey)
			if err == nil {
				statusRetrieved = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !statusRetrieved {
			t.Fatalf("Could not retrieve status for the pod %d: %s", i, err)
		}

		found := false
		for _, processStatus := range status.ProcessStatuses {
			if processStatus.LaunchableID == finish.LaunchableID && processStatus.LaunchScriptName == finish.EntryPoint {
				found = true
				if processStatus.LastExit.ExitStatus != finish.ExitStatus {
					t.Errorf("Expected exit status of %d but got %d for pod %d", finish.ExitStatus, processStatus.LastExit.ExitStatus, i)
				}

				if processStatus.LastExit.ExitCode != finish.ExitCode {
					t.Errorf("Expected exit code of %d but got %d for pod %d", finish.ExitCode, processStatus.LastExit.ExitCode, i)
				}
			}
		}

		if !found {
			t.Error("Did not find the first exited process in the status store")
		}
	}

}

func timeoutWriteMessage(t *testing.T, sockPath string, message []byte) {
	base64Bytes := make([]byte, base64.StdEncoding.EncodedLen(len(message)))
	base64.StdEncoding.Encode(base64Bytes, message)
	finishCh := make(chan struct{})
	go func() {
		c, err := net.Dial("unix", sockPath)
		if err != nil {
			t.Fatalf("Unable to dial socket: %s", err)
		}
		defer c.Close()
		_, err = c.Write(base64Bytes)
		if err != nil {
			t.Fatalf("Unable to write to socket: %s", err)
		}

		close(finishCh)
	}()

	select {
	case <-finishCh:
	case <-time.After(1 * time.Second):
		t.Fatalf("Could not write %s to socket after 1 second", string(message))
	}
}

func startReporter(t *testing.T, tempDir string) (string, chan struct{}, podstatus.Store) {
	sockPath := filepath.Join(tempDir, "finish.sock")
	config := ReporterConfig{
		ProcessResultSocketPath: sockPath,
		// This setting only matters for generating the FinishExec in the preparer,
		// it won't affect the tests in this file
		EnvironmentExtractorPath: "foo",
	}

	store := podstatus.NewConsul(statusstoretest.NewFake(), kp.PreparerPodStatusNamespace)
	reporter, ok := New(config, logging.DefaultLogger, store)
	if !ok {
		t.Fatal("Config was not sufficient to create a reporter")
	}
	quitCh := make(chan struct{})

	// In production this is run with 0622 permissions, which won't work
	// for the test, so we call the private function that exposes the
	// FileMode arg
	go reporter.run(quitCh, 0777)

	// wait for the reporter to start listening on the socket
	reporterUp := make(chan struct{})
	reporterUpQuit := make(chan struct{})
	defer close(reporterUpQuit)
	go func() {
		for {
			select {
			case <-reporterUpQuit:
				return
			default:
			}

			c, err := net.Dial("unix", sockPath)
			if err == nil {
				c.Close()
				close(reporterUp)
				return
			}
		}
	}()

	select {
	case <-reporterUp:
	case <-time.After(1 * time.Second):
		t.Fatal("Reporter did not listen on socket after 1 second")
	}

	return sockPath, quitCh, store
}
