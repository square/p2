package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	label_grpc_client "github.com/square/p2/pkg/grpc/labelstore/client"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/preparer/podprocess"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/schedule"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/podstore"
	"github.com/square/p2/pkg/store/consul/rcstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/podstatus"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
	"google.golang.org/grpc"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	preparerStatusPort       = 32170
	certpath                 = "/var/tmp/certs"
	sqliteFinishDatabasePath = "/data/pods/p2-preparer/finish_data/finish.db"
)

func main() {
	// 1. Generate pod for preparer in this code version (`rake artifact:prepare`)
	// 2. Locate manifests for preparer pod, premade consul pod
	// 3. Execute bootstrap with premade consul pod and preparer pod
	// 4. Delete all pods from the pod store (uuid pods). This allows the same vagrant VM to be used
	// between tests
	// 5. Deploy p2-rctl-server pod with p2-schedule
	// 6. Schedule a hello pod manifest with a replication controller
	// 7. Schedule a hello pod as a "uuid pod"
	// 8. Verify that p2-rctl-server is running by checking health.
	// 9. Verify that the RC-deployed hello is running by checking health.
	// Monitor using written pod label queries.
	// 10. Verify that the uuid hello pod is running by curling its HTTP port.
	// Health is not checked for uuid pods so checking health cannot be used.

	// list of services running on integration test host
	services := []string{"p2-preparer", "hello"}
	tempdir, err := ioutil.TempDir("", "single-node-check")
	log.Printf("Putting test manifests in %s\n", tempdir)
	if err != nil {
		log.Fatalln("Could not create temp directory, bailing")
	}

	requireFile, err := createRequireFile(tempdir)
	if err != nil {
		log.Fatalln("Could not create temp require file, bailing: %s", err)
	}

	userHookManifest, err := userCreationHookManifest(tempdir)
	if err != nil {
		log.Fatalf("Couldn't schedule the user creation hook: %s", err)
	}

	preparerManifest, err := generatePreparerPod(tempdir, userHookManifest, requireFile)
	if err != nil {
		log.Fatalf("Could not generate preparer pod: %s\n", err)
	}
	config, err := preparer.LoadConfig(preparerManifest)
	if err != nil {
		log.Fatalf("could not unmarshal config: %s\n", err)
	}

	consulManifest, err := getConsulManifest(tempdir)
	if err != nil {
		log.Fatalf("Could not generate consul pod: %s\n", err)
	}
	signedPreparerManifest, err := signManifest(preparerManifest, tempdir)
	if err != nil {
		log.Fatalf("Could not sign preparer manifest: %s\n", err)
	}
	signedConsulManifest, err := signManifest(consulManifest, tempdir)
	if err != nil {
		log.Fatalf("Could not sign consul manifest: %s\n", err)
	}

	fmt.Println("Executing bootstrap")
	err = executeBootstrap(signedPreparerManifest, signedConsulManifest, requireFile)
	if err != nil {
		log.Fatalf("Could not execute bootstrap: %s\n%s\n%s", err, targetLogs("p2-preparer"), targetLogs("consul"))
	}

	// Wait a bit for preparer's http server to be ready
	err = waitForStatus(preparerStatusPort, "preparer", 10*time.Second)
	if err != nil {
		log.Fatalf("Couldn't check preparer status: %s", err)
	}

	consulClient := consul.NewConsulClient(consul.Options{})
	// Get all the pod unique keys so we can unschedule them all
	keys, _, err := consulClient.KV().Keys(podstore.PodTree+"/", "", nil)
	if err != nil {
		log.Fatalf("Could not fetch pod keys to remove from store at beginning of test: %s", err)
	}

	podStore := podstore.NewConsul(consulClient.KV())
	for _, key := range keys {
		keyParts := strings.Split(key, "/")
		err = podStore.Unschedule(types.PodUniqueKey(keyParts[len(keyParts)-1]))
		if err != nil {
			log.Fatalf("Could not unschedule pod %s from consul: %s", keyParts[len(keyParts)-1], err)
		}
	}

	err = startLabelStoreServer(tempdir)
	if err != nil {
		log.Fatalf("Could not start grpc label store: %s", err)
	}

	err = scheduleRCTLServer(tempdir)
	if err != nil {
		log.Fatalf("Could not schedule RCTL server: %s", err)
	}

	// Now we're going to test some conditions that each take non-negligible amount of time to verify.
	// We'll spin up a goroutine for each "test" which either closes the error channel, or passes an error.
	type testCase struct {
		testName string
		errCh    chan error
		logger   logging.Logger
	}

	var testCases []testCase

	// Test that a "legacy" pod installed by an RC comes up correctly and has health reported
	legacyTest := make(chan error)
	verifyLegacyPodLogger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"test_case": "verifyLegacyPod",
	})
	go verifyLegacyPod(legacyTest, tempdir, config, services, verifyLegacyPodLogger)
	testCases = append(testCases, testCase{
		testName: "verifyLegacyPod",
		errCh:    legacyTest,
		logger:   verifyLegacyPodLogger,
	})

	// Test that a "uuid" pod installed by p2-schedule comes up correctly
	uuidTest := make(chan error)
	verifyUUIDPodLogger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"test_case": "verifyUUIDPod",
	})
	testCases = append(testCases, testCase{
		testName: "verifyUUIDPod",
		errCh:    uuidTest,
		logger:   verifyUUIDPodLogger,
	})
	go verifyUUIDPod(uuidTest, tempdir, verifyUUIDPodLogger)

	// Test that exit information for a process started by a pod is properly recorded in consul.
	processExitTest := make(chan error)
	verifyProcessExitLogger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"test_case": "verifyProcessExit",
	})
	go verifyProcessExit(processExitTest, tempdir, verifyProcessExitLogger)
	testCases = append(testCases, testCase{
		testName: "verifyProcessResult",
		errCh:    processExitTest,
		logger:   verifyProcessExitLogger,
	})

	// Test that the consul rc store properly performs atomic "transfer" of
	// replicas desired from one RC to another.
	transferReplicasTest := make(chan error)
	verifyTransferReplicasLogger := logging.DefaultLogger.SubLogger(logrus.Fields{
		"test_case": "verifyTransferReplicas",
	})
	go verifyTransferReplicas(transferReplicasTest, tempdir, verifyTransferReplicasLogger, consulClient)
	testCases = append(testCases, testCase{
		testName: "verifyTransferReplicas",
		errCh:    transferReplicasTest,
		logger:   verifyTransferReplicasLogger,
	})

	for _, t := range testCases {
		select {
		case err, ok := <-t.errCh:
			if err != nil {
				t.logger.Fatal(err)
			}
			if ok {
				t.logger.Fatal("Error channel not closed")
			}
		case <-time.After(3 * time.Minute):
			t.logger.Fatal("Timed out waiting for a result")
		}

		t.logger.Infoln("Success!")
	}
}

func verifyLegacyPod(errCh chan error, tempDir string, config *preparer.PreparerConfig, services []string, logger logging.Logger) {
	defer close(errCh)
	// Schedule a "legacy" hello pod using a replication controller
	rcID, err := createHelloReplicationController(tempDir)
	if err != nil {
		errCh <- fmt.Errorf("Could not create hello pod / rc: %s", err)
		return
	}
	logger.Infof("Created RC #%s for hello\n", rcID)

	err = waitForPodLabeledWithRC(klabels.Everything().Add(rc.RCIDLabel, klabels.EqualsOperator, []string{rcID.String()}), rcID)
	if err != nil {
		errCh <- fmt.Errorf("Failed waiting for pods labeled with the given RC: %v", err)
		return
	}

	logger.Infoln("RC successfully scheduled pod")

	err = verifyHelloRunning("", logger)
	if err != nil {
		errCh <- fmt.Errorf("Couldn't get hello running: %s", err)
		return
	}

	logger.Infoln("hello pod is running")

	err = verifyHealthChecks(config, services)
	if err != nil {
		errCh <- fmt.Errorf("Could not get health check info from consul: %s", err)
		return
	}
	logger.Infof("health checks for %s succeeded", services)
}

func verifyUUIDPod(errCh chan error, tempDir string, logger logging.Logger) {
	defer close(errCh)

	// Schedule a "uuid" hello pod on a different port
	podUniqueKey, err := createHelloUUIDPod(tempDir, 43771, logger)
	if err != nil {
		errCh <- fmt.Errorf("Could not schedule UUID hello pod: %s", err)
		return
	}
	logger.Infoln("p2-schedule'd another hello instance as a uuid pod running on port 43771")

	err = verifyHelloRunning(podUniqueKey, logger)
	if err != nil {
		errCh <- fmt.Errorf("Couldn't get hello running as a uuid pod: %s", err)
		return
	}
}

func verifyProcessExit(errCh chan error, tempDir string, logger logging.Logger) {
	defer close(errCh)

	// Schedule a uuid pod
	podUniqueKey, err := createHelloUUIDPod(tempDir, 43772, logger)
	if err != nil {
		errCh <- fmt.Errorf("Could not schedule UUID hello pod: %s", err)
		return
	}

	logger = logger.SubLogger(logrus.Fields{
		"pod_unique_key": podUniqueKey,
	})
	logger.Infoln("Scheduled hello instance on port 43772")

	err = verifyHelloRunning(podUniqueKey, logger)
	if err != nil {
		errCh <- fmt.Errorf("Couldn't get hello running as a uuid pod: %s", err)
		return
	}
	logger.Infoln("Hello instance launched")

	time.Sleep(3 * time.Second)

	logger.Infoln("Waiting for hello instance to listen on 43772")
	// now wait for the hello server to start running
	timeout := time.After(30 * time.Second)
	for {
		resp, err := http.Get("http://localhost:43772/")
		if err == nil {
			resp.Body.Close()
			break
		}

		select {
		case <-timeout:
			errCh <- fmt.Errorf("Hello didn't come up listening on 43772: %s", err)
			return
		default:
		}

		time.Sleep(1 * time.Second)
	}

	exitCode := rand.Intn(100) + 1
	logger.Infof("Causing hello on 43772 to exit with status %d", exitCode)
	// Make an http request to hello to make it exit with exitCode. We expect the http request to fail due
	// to the server exiting, so don't check for http errors.
	_, err = http.Get(fmt.Sprintf("http://localhost:43772/exit/%d", exitCode))
	if err == nil {
		// This is bad, it means the hello server didn't die and kill our request
		// in the middle
		errCh <- util.Errorf("Couldn't kill hello server with http request")
		return
	}

	urlError, ok := err.(*url.Error)
	if ok && urlError.Err == io.EOF {
		// This is good, it means the server died
	} else {
		errCh <- fmt.Errorf("Couldn't tell hello to die over http: %s", err)
		return
	}

	logger.Infoln("Checking for exit code in SQL database")
	finishService, err := podprocess.NewSQLiteFinishService(sqliteFinishDatabasePath, logging.DefaultLogger)
	if err != nil {
		errCh <- err
		return
	}

	var finishResult podprocess.FinishOutput
	timeout = time.After(30 * time.Second)
	for {
		finishResult, err = finishService.LastFinishForPodUniqueKey(podUniqueKey)
		if err == nil {
			break
		}

		select {
		case <-timeout:
			// Try to manually run the finish script in order to make debugging the test failure easier
			output, err := exec.Command("sudo", fmt.Sprintf("/var/service/hello-%s__hello__launch/finish", podUniqueKey), "1", "2").CombinedOutput()
			if err != nil {
				logger.WithError(err).Infoln("DEBUG: Debug attempt to run finish script failed")
			}

			logger.Infof("DEBUG: Output of direct execution of finish script: %s", string(output))

			errCh <- fmt.Errorf("Did not find a finish row by the deadline: %s", err)
			return
		default:
		}
	}

	if finishResult.PodUniqueKey != podUniqueKey {
		errCh <- fmt.Errorf("Expected finish result for '%s' but it was for '%s'", podUniqueKey, finishResult.PodUniqueKey)
		return
	}

	if finishResult.ExitCode != exitCode {
		errCh <- fmt.Errorf("Exit code for '%s' in the sqlite database was expected to be %d but was %d", podUniqueKey, exitCode, finishResult.ExitCode)
		return
	}

	logger.Infoln("Checking for exit code in consul")
	timeout = time.After(30 * time.Second)
	podStatusStore := podstatus.NewConsul(statusstore.NewConsul(consul.NewConsulClient(consul.Options{})), consul.PreparerPodStatusNamespace)
	for {

		podStatus, _, err := podStatusStore.Get(podUniqueKey)
		if err != nil {
			errCh <- err
			return
		}

		found := false
		for _, processStatus := range podStatus.ProcessStatuses {
			if processStatus.LaunchableID == "hello" && processStatus.EntryPoint == "bin/launch" {
				found = true
				if processStatus.LastExit == nil {
					errCh <- fmt.Errorf("Found no last exit in consul pod status for %s", podUniqueKey)
					return
				}

				if processStatus.LastExit.ExitCode != exitCode {
					errCh <- fmt.Errorf("Exit code for '%s' in consul was expected to be %d but was %d", podUniqueKey, exitCode, finishResult.ExitCode)
					return
				}
			}
		}

		if found {
			logger.Infoln("Successful!")
			break
		}

		select {
		case <-timeout:
			errCh <- fmt.Errorf("There was no pod process for hello/launch for %s in consul", podUniqueKey)
			return
		default:
		}
	}
}

func verifyTransferReplicas(errCh chan<- error, tempdir string, logger logging.Logger, consulClient consulutil.ConsulClient) {
	defer close(errCh)

	applicator := labels.NewConsulApplicator(consulClient, 1)
	rcStore := rcstore.NewConsul(consulClient, applicator, 2)

	builder := manifest.NewBuilder()
	builder.SetID("some_pod")
	man := builder.GetManifest()

	fromRC, err := rcStore.Create(man, klabels.Everything(), "some_az", "some_cn", nil, nil)
	if err != nil {
		errCh <- util.Errorf("could not create RC for replica transfer test: %s", err)
		return
	}

	toRC, err := rcStore.Create(man, klabels.Everything(), "some_az", "some_cn", nil, nil)
	if err != nil {
		errCh <- util.Errorf("could not create second RC for replica transfer test: %s", err)
		return
	}

	err = rcStore.SetDesiredReplicas(fromRC.ID, 4)
	if err != nil {
		errCh <- util.Errorf("could not initialize replica count for replica transfer test: %s", err)
		return
	}

	replicasToAdd := 1
	replicasToRemove := 2
	startingFromReplicas := 4
	startingToReplicas := 0
	transferReq := rcstore.TransferReplicaCountsRequest{
		ToRCID:               toRC.ID,
		ReplicasToAdd:        &replicasToAdd,
		FromRCID:             fromRC.ID,
		ReplicasToRemove:     &replicasToRemove,
		StartingFromReplicas: &startingFromReplicas,
		StartingToReplicas:   &startingToReplicas,
	}
	err = rcStore.TransferReplicaCounts(transferReq)
	if err != nil {
		errCh <- util.Errorf("failed to transfer replicas from one RC to another: %s", err)
		return
	}

	fromRC, err = rcStore.Get(fromRC.ID)
	if err != nil {
		errCh <- util.Errorf("could not fetch original RC to verify replica counts: %s", err)
		return
	}

	toRC, err = rcStore.Get(toRC.ID)
	if err != nil {
		errCh <- util.Errorf("could not fetch second RC to verify replica counts: %s", err)
		return
	}

	if fromRC.ReplicasDesired != 2 || toRC.ReplicasDesired != 1 {
		errCh <- util.Errorf("expected first RC to have %d, and second RC to have %d replicas, but they actually had %d and %d", 2, 1, fromRC.ReplicasDesired, toRC.ReplicasDesired)
		return
	}
}

func signManifest(manifestPath string, workdir string) (string, error) {
	signedManifestPath := fmt.Sprintf("%s.asc", manifestPath)
	return signedManifestPath,
		exec.Command("gpg", "--no-default-keyring",
			"--keyring", util.From(runtime.Caller(0)).ExpandPath("pubring.gpg"),
			"--secret-keyring", util.From(runtime.Caller(0)).ExpandPath("secring.gpg"),
			"-u", "p2universe",
			"--output", signedManifestPath,
			"--clearsign", manifestPath).Run()
}

func signBuild(artifactPath string) error {
	sigLoc := fmt.Sprintf("%s.sig", artifactPath)
	output, err := exec.Command("gpg", "--no-default-keyring",
		"--keyring", util.From(runtime.Caller(0)).ExpandPath("pubring.gpg"),
		"--secret-keyring", util.From(runtime.Caller(0)).ExpandPath("secring.gpg"),
		"-u", "p2universe",
		"--out", sigLoc,
		"--detach-sign", artifactPath).CombinedOutput()
	if err != nil {
		fmt.Println(string(output))
		return err
	}

	return nil
}

func generatePreparerPod(workdir string, userHookManifest manifest.Manifest, requireFile string) (string, error) {
	// build the artifact from HEAD
	output, err := exec.Command("go", "build", "github.com/square/p2/bin/p2-preparer").CombinedOutput()
	if err != nil {
		return "", util.Errorf("Couldn't build preparer: %s\nOutput:\n%s", err, string(output))
	}
	wd, _ := os.Getwd()
	hostname, err := os.Hostname()
	if err != nil {
		return "", util.Errorf("Couldn't get hostname: %s", err)
	}
	// the test number forces the pod manifest to change every test run.
	testNumber := fmt.Sprintf("test=%d", rand.Intn(2000000000))
	cmd := exec.Command("p2-bin2pod", "--work-dir", workdir, "--id", "p2-preparer", "--config", fmt.Sprintf("node_name=%s", hostname), "--config", testNumber, wd+"/p2-preparer")
	prepBin2Pod, err := executeBin2Pod(cmd)
	if err != nil {
		return "", err
	}

	if err = signBuild(prepBin2Pod.TarPath); err != nil {
		return "", err
	}

	manifest, err := manifest.FromPath(prepBin2Pod.ManifestPath)
	if err != nil {
		return "", err
	}
	builder := manifest.GetBuilder()
	builder.SetID("p2-preparer")

	envExtractorPath, err := exec.Command("which", "p2-finish-env-extractor").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("Could not find p2-finish-env-extractor on PATH")
	}

	userCreationHookBytes, err := userHookManifest.Marshal()
	if err != nil {
		return "", util.Errorf("Couldn't marshal user hook manifest: %s", err)
	}

	err = builder.SetConfig(map[interface{}]interface{}{
		"preparer": map[interface{}]interface{}{
			"auth": map[string]string{
				"type":    "keyring",
				"keyring": util.From(runtime.Caller(0)).ExpandPath("pubring.gpg"),
			},
			"artifact_auth": map[interface{}]interface{}{
				"type":    "build",
				"keyring": util.From(runtime.Caller(0)).ExpandPath("pubring.gpg"),
			},
			"ca_file":     filepath.Join(certpath, "cert.pem"),
			"cert_file":   filepath.Join(certpath, "cert.pem"),
			"key_file":    filepath.Join(certpath, "key.pem"),
			"status_port": preparerStatusPort,
			"process_result_reporter_config": map[string]string{
				"sqlite_database_path":       sqliteFinishDatabasePath,
				"environment_extractor_path": strings.TrimSpace(string(envExtractorPath)),
				"workspace_dir_path":         "/data/pods/p2-preparer/tmp",
			},
			"hooks_manifest": string(userCreationHookBytes),
			"require_file":   requireFile,
		},
	})
	if err != nil {
		return "", err
	}

	builder.SetRunAsUser("root")
	builder.SetStatusPort(preparerStatusPort)
	builder.SetStatusHTTP(true)

	manifest = builder.GetManifest()

	manifestBytes, err := manifest.Marshal()
	if err != nil {
		return "", err
	}

	err = ioutil.WriteFile(prepBin2Pod.ManifestPath, manifestBytes, 0644)
	if err != nil {
		return "", err
	}

	return prepBin2Pod.ManifestPath, err
}

func checkStatus(statusPort int, pod string) error {
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/_status", statusPort))
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return util.Errorf("Did not get OK response from %s: %s %s", pod, resp.Status, string(body))
	} else {
		log.Printf("Status of %s: %s", pod, string(body))
	}
	return nil
}

func waitForStatus(statusPort int, pod string, waitTime time.Duration) error {
	successCh := make(chan struct{})

	var err error

	go func() {
		for range time.Tick(1 * time.Second) {
			err = checkStatus(statusPort, pod)
			if err == nil {
				close(successCh)
				break
			}
		}
	}()

	select {
	case <-time.After(waitTime):
		return err
	case <-successCh:
		return nil
	}
}

func userCreationHookManifest(tmpdir string) (manifest.Manifest, error) {
	createUserPath := path.Join(tmpdir, "create_user")
	script := `#!/usr/bin/env bash
set -e
mkdir -p $HOOKED_POD_HOME
/sbin/adduser $HOOKED_POD_ID -d $HOOKED_POD_HOME
`
	err := ioutil.WriteFile(createUserPath, []byte(script), 0744)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("p2-bin2pod", "--work-dir", tmpdir, createUserPath)
	createUserBin2Pod, err := executeBin2Pod(cmd)
	if err != nil {
		return nil, err
	}

	if err = signBuild(createUserBin2Pod.TarPath); err != nil {
		return nil, err
	}
	manifestPath := createUserBin2Pod.ManifestPath

	userHookManifest, err := manifest.FromPath(manifestPath)
	if err != nil {
		return nil, err
	}

	builder := userHookManifest.GetBuilder()

	builder.SetRunAsUser("root")
	userHookManifest = builder.GetManifest()
	return userHookManifest, nil
}

type Bin2PodResult struct {
	TarPath       string `json:"tar_path"`
	ManifestPath  string `json:"manifest_path"`
	FinalLocation string `json:"final_location"`
}

func executeBin2Pod(cmd *exec.Cmd) (Bin2PodResult, error) {
	out := bytes.Buffer{}
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return Bin2PodResult{}, util.Errorf("p2-bin2pod failed: %s", err)
	}
	var bin2podres Bin2PodResult
	err = json.Unmarshal(out.Bytes(), &bin2podres)
	if err != nil {
		return Bin2PodResult{}, err
	}
	return bin2podres, nil
}

func getConsulManifest(dir string) (string, error) {
	_, err := os.Stat("/usr/bin/consul")
	if err != nil && !os.IsNotExist(err) {
		return "", util.Errorf("could not deploy consul because stat /usr/bin/consul failed: %s", err)
	} else if os.IsNotExist(err) {
		consulURI, err := url.Parse("https://releases.hashicorp.com/consul/0.7.1/consul_0.7.1_linux_amd64.zip")
		if err != nil {
			return "", util.Errorf("could not download consul: %s", err)
		}

		dst := filepath.Join(dir, "consul_zip")
		err = uri.URICopy(consulURI, dst)
		if err != nil {
			return "", util.Errorf("could not download conzul: %s", err)
		}

		output, err := exec.Command("unzip", dst, "-d", dir).CombinedOutput()
		if err != nil {
			return "", util.Errorf("could not unzip the consul zip archive: %s\n%s", err, string(output))
		}

		output, err = exec.Command("mv", filepath.Join(dir, "consul"), "/usr/bin/consul").CombinedOutput()
		if err != nil {
			return "", util.Errorf("could not move consul binary to /usr/bin/consul: %s\n%s", err, string(output))
		}
	}

	interfaces, err := net.Interfaces()
	if err != nil {
		return "", util.Errorf("could not list host interfaces to find which to bind consul to: %s", err)
	}
	addrs, err := interfaces[0].Addrs()
	if err != nil {
		return "", util.Errorf("could not list addresses to find one to bind consul to: %s", err)
	}
	ip := strings.Split(addrs[0].String(), "/")[0]

	rubyBin := filepath.Join(dir, "launch")
	cmds := `#!/usr/bin/env ruby

exec "/usr/bin/consul agent -server -bootstrap-expect 1 -data-dir /tmp/consul -bind %%%%"`
	cmds = strings.Replace(cmds, "%%%%", ip, -1)
	err = ioutil.WriteFile(rubyBin, []byte(cmds), 0777)
	if err != nil {
		return "", util.Errorf("could not write consul exec script: %s", err)
	}

	cmd := exec.Command("p2-bin2pod", "--work-dir", dir, rubyBin, "--id", "consul")
	rctlBin2Pod, err := executeBin2Pod(cmd)
	if err != nil {
		return "", err
	}

	return rctlBin2Pod.ManifestPath, nil
}

func executeBootstrap(preparerManifest, consulManifest string, requireFile string) error {
	_, err := user.Lookup("consul")
	if _, ok := err.(user.UnknownUserError); ok {
		err = exec.Command("sudo", "useradd", "consul").Run()
		if err != nil {
			return fmt.Errorf("Could not create consul user: %s", err)
		}
	}

	cmd := exec.Command("rake", "install")
	cmd.Stderr = os.Stdout
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("Could not install newest bootstrap: %s", err)
	}
	bootstr := exec.Command("p2-bootstrap", "--consul-pod", consulManifest, "--agent-pod", preparerManifest, "--consul-timeout", "20s", "--require-file", requireFile)
	bootstr.Stdout = os.Stdout
	bootstr.Stderr = os.Stdout
	return bootstr.Run()
}

func scheduleRCTLServer(dir string) error {
	p2RCTLServerPath, err := exec.Command("which", "p2-rctl-server").CombinedOutput()
	if err != nil {
		return fmt.Errorf("Could not find p2-rctl-server on PATH")
	}
	chomped := strings.TrimSpace(string(p2RCTLServerPath))
	if _, err = os.Stat(chomped); os.IsNotExist(err) {
		return fmt.Errorf("%v does not exist", chomped)
	}
	cmd := exec.Command("p2-bin2pod", "--work-dir", dir, chomped)
	rctlBin2Pod, err := executeBin2Pod(cmd)
	if err != nil {
		return err
	}
	if err = signBuild(rctlBin2Pod.TarPath); err != nil {
		return err
	}

	signedPath, err := signManifest(rctlBin2Pod.ManifestPath, dir)
	if err != nil {
		return err
	}
	return exec.Command("p2-schedule", signedPath).Run()
}

// Writes a pod manifest for the hello pod at with the specified name in the
// specified dir, configured to run on the specified port. Returns the path to
// the signed manifest
func writeHelloManifest(dir string, manifestName string, port int) (string, error) {
	hello := fmt.Sprintf("file://%s", util.From(runtime.Caller(0)).ExpandPath("../hoisted-hello_def456.tar.gz"))
	builder := manifest.NewBuilder()
	builder.SetID("hello")
	builder.SetStatusPort(port)
	builder.SetStatusHTTP(true)
	stanzas := map[launch.LaunchableID]launch.LaunchableStanza{
		"hello": {
			LaunchableType: "hoist",
			Location:       hello,
		},
	}
	builder.SetLaunchables(stanzas)
	builder.SetConfig(map[interface{}]interface{}{
		"port": port,
	})
	manifest := builder.GetManifest()

	manifestPath := filepath.Join(dir, manifestName)
	f, err := os.OpenFile(manifestPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	defer f.Close()
	err = manifest.Write(f)
	if err != nil {
		return "", err
	}

	return signManifest(manifestPath, dir)
}

func createHelloUUIDPod(dir string, port int, logger logging.Logger) (types.PodUniqueKey, error) {
	signedManifestPath, err := writeHelloManifest(dir, fmt.Sprintf("hello-uuid-%d.yaml", port), port)
	if err != nil {
		return "", err
	}

	logger.Infoln("Scheduling uuid pod")
	cmd := exec.Command("p2-schedule", "--uuid-pod", signedManifestPath)
	stdout := bytes.Buffer{}
	stderr := bytes.Buffer{}
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	if err != nil {
		fmt.Println(stderr.String())
		return "", err
	}

	var out schedule.Output
	err = json.Unmarshal(stdout.Bytes(), &out)
	if err != nil {
		return "", util.Errorf("Scheduled uuid pod but couldn't parse uuid from p2-schedule output: %s", err)
	}

	logger.Infof("Scheduled uuid pod %s", out.PodUniqueKey)
	return out.PodUniqueKey, nil
}

func createHelloReplicationController(dir string) (fields.ID, error) {
	signedManifestPath, err := writeHelloManifest(dir, "hello.yaml", 43770)
	if err != nil {
		return "", err
	}

	cmd := exec.Command(
		"p2-rctl",
		"--log-json",
		"create",
		"--manifest",
		signedManifestPath,
		"--node-selector",
		"test=yes",
		"--availability-zone",
		"some_az",
		"--cluster-name",
		"some_cn",
	)
	out := bytes.Buffer{}
	cmd.Stdout = &out
	cmd.Stderr = &out
	err = cmd.Run()
	if err != nil {
		return fields.ID(""), fmt.Errorf("Couldn't create replication controller for hello: %s %s", out.String(), err)
	}
	var rctlOut struct {
		ID string `json:"id"`
	}

	err = json.Unmarshal(out.Bytes(), &rctlOut)
	if err != nil {
		return fields.ID(""), fmt.Errorf("Couldn't read RC ID out of p2-rctl invocation result: %v", err)
	}

	output, err := exec.Command("p2-rctl", "set-replicas", rctlOut.ID, "1", "-y").CombinedOutput()
	if err != nil {
		fmt.Println(string(output))
		return "", err
	}
	return fields.ID(rctlOut.ID), nil
}

func waitForPodLabeledWithRC(selector klabels.Selector, rcID fields.ID) error {
	conn, err := grpc.Dial("localhost:3000", grpc.WithBlock(), grpc.WithTimeout(5*time.Second), grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("Could not connect to grpc server: %s", err)
	}
	grpcClient := label_grpc_client.NewClient(conn, logging.DefaultLogger)

	// we have to label this hostname as being allowed to run tests
	host, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("Could not get hostname: %s", err)
	}

	client := consul.NewConsulClient(consul.Options{})
	applicator := labels.NewConsulApplicator(client, 1)

	err = applicator.SetLabel(labels.NODE, host, "test", "yes")
	if err != nil {
		return fmt.Errorf("Could not set node selector label on %s: %v", host, err)
	}

	// Set a dummy POD label to circumvent the various label failsafes
	// that ignore missing data for a whole label type
	err = applicator.SetLabel(labels.POD, fmt.Sprintf("%s/%s", host, "some_pod"), "test", "yes")
	if err != nil {
		return fmt.Errorf("Could not set node selector label on %s: %v", host, err)
	}

	quitCh := make(chan struct{})
	defer close(quitCh)

	watchCh, err := grpcClient.WatchMatches(selector, labels.POD, 0, quitCh)
	if err != nil {
		return fmt.Errorf("Could not initialize labels grpc client: %s", err)
	}

	waitTime := time.After(30 * time.Second)
	for {
		select {
		case <-waitTime:
			return fmt.Errorf("Label selector %v wasn't matched before timeout: %s%s", selector, targetLogs("hello"), targetLogs("p2-preparer"))
		case res, ok := <-watchCh:
			if !ok {
				return fmt.Errorf("Label selector watch unexpectedly terminated")
			}
			if len(res) > 1 {
				return fmt.Errorf("Too many results found, should only have 1: %v", res)
			}
			if len(res) == 1 {
				_, podID, err := labels.NodeAndPodIDFromPodLabel(res[0])
				if err != nil {
					return err
				}
				if podID.String() != "hello" {
					return fmt.Errorf("Should have found the hello pod, instead found %s", podID)
				}
				return nil
			}
		}
	}
}

func verifyHelloRunning(podUniqueKey types.PodUniqueKey, logger logging.Logger) error {
	helloPidAppeared := make(chan struct{})
	quit := make(chan struct{})
	defer close(quit)

	serviceDir := "/var/service/hello__hello__launch"
	if podUniqueKey != "" {
		serviceDir = fmt.Sprintf("/var/service/hello-%s__hello__bin__launch", podUniqueKey)
	}
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			res := exec.Command("sudo", "sv", "stat", serviceDir).Run()
			if res == nil {
				select {
				case <-quit:
					logger.Infoln("got a valid stat after timeout")
				case helloPidAppeared <- struct{}{}:
				}
				return
			} else {
				select {
				case <-quit:
					return
				default:
				}
			}
		}
	}()
	select {
	case <-time.After(30 * time.Second):
		return fmt.Errorf("Couldn't start hello after 30 seconds:\n\n %s%s", targetLogs("hello"), targetLogs("p2-preparer"))
	case <-helloPidAppeared:
		return nil
	}
}

func verifyHelloUUIDRunning(podUniqueKey types.PodUniqueKey) error {
	helloUUIDAppeared := make(chan struct{})
	quit := make(chan struct{})
	defer close(quit)
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			err := exec.Command("curl", "localhost:43771").Run()
			if err == nil {
				select {
				case <-quit:
					fmt.Println("got a valid curl after timeout")
				case helloUUIDAppeared <- struct{}{}:
				}
				return
			} else {
				select {
				case <-quit:
					return
				default:
				}
			}
		}
	}()
	select {
	case <-time.After(1 * time.Minute):
		return fmt.Errorf("hello-%s didn't respond healthy on port 43771 after 60 seconds:\n\n %s", podUniqueKey, targetUUIDLogs(podUniqueKey))
	case <-helloUUIDAppeared:
		return nil
	}
}

func targetUUIDLogs(podUniqueKey types.PodUniqueKey) string {
	var helloUUIDTail bytes.Buffer
	helloT := exec.Command("tail", fmt.Sprintf("/var/service/hello-%s__hello__bin__launch/log/main/current", podUniqueKey))
	helloT.Stdout = &helloUUIDTail
	helloT.Run()
	return fmt.Sprintf("hello uuid tail: \n%s\n\n", helloUUIDTail.String())
}

func targetLogs(app string) string {
	output, err := exec.Command("tail", fmt.Sprintf("/var/service/%s__%s__launch/log/main/current", app, app)).CombinedOutput()
	if err != nil {
		log.Printf("Tail failed: %s", err)
	}
	return fmt.Sprintf("%s tail: \n%s\n\n", app, string(output))
}

func verifyHealthChecks(config *preparer.PreparerConfig, services []string) error {
	client, err := config.GetConsulClient()
	if err != nil {
		return err
	}
	store := consul.NewConsulStore(client)

	time.Sleep(30 * time.Second)
	// check consul for health information for each app
	name, err := os.Hostname()
	if err != nil {
		return err
	}

	node := types.NodeName(name)
	for _, sv := range services {
		res, err := store.GetHealth(sv, node)
		if err != nil {
			return err
		} else if (res == consul.WatchResult{}) {
			return fmt.Errorf("No results for %s: \n\n %s%s", sv, targetLogs("hello"), targetLogs("p2-preparer"))
		} else if res.Status != string(health.Passing) {
			return fmt.Errorf("%s did not pass health check: \n\n %s%s", sv, targetLogs("hello"), targetLogs("p2-preparer"))
		} else {
			fmt.Println(res)
		}
	}

	for _, sv := range services {
		res, err := store.GetServiceHealth(sv)
		getres, _ := store.GetHealth(sv, node)
		if err != nil {
			return err
		}
		val := res[consul.HealthPath(sv, node)]
		if getres.Id != val.Id || getres.Service != val.Service || getres.Status != val.Status {
			return fmt.Errorf("GetServiceHealth failed %+v: \n\n%s%s", res, targetLogs("hello"), targetLogs("p2-preparer"))
		}
	}

	// if it reaches here it means health checks
	// are being written to the KV store properly
	return nil
}

func startLabelStoreServer(dir string) error {
	labelStoreServerPath, err := exec.Command("which", "label-store-server").CombinedOutput()
	if err != nil {
		return fmt.Errorf("Could not find label-store-server on PATH")
	}
	chomped := strings.TrimSpace(string(labelStoreServerPath))
	if _, err = os.Stat(chomped); os.IsNotExist(err) {
		return fmt.Errorf("%v does not exist", chomped)
	}
	cmd := exec.Command("p2-bin2pod", "--work-dir", dir, chomped)
	labelStoreBin2Pod, err := executeBin2Pod(cmd)
	if err != nil {
		return err
	}

	if err = signBuild(labelStoreBin2Pod.TarPath); err != nil {
		return err
	}

	signedPath, err := signManifest(labelStoreBin2Pod.ManifestPath, dir)
	if err != nil {
		return err
	}

	err = exec.Command("p2-schedule", signedPath).Run()
	if err != nil {
		return err
	}

	// Test connection
	conn, err := grpc.Dial("localhost:3000", grpc.WithBlock(), grpc.WithInsecure(), grpc.WithTimeout(15*time.Second))
	if err != nil {
		fmt.Println(targetLogs("label-store-server"))
		fmt.Println(targetLogs("p2-preparer"))
		return err
	}
	conn.Close()

	return nil
}

func createRequireFile(dir string) (string, error) {
	f, err := ioutil.TempFile(dir, "may-run")
	if err != nil {
		return "", err
	}

	// make directory executable by all.
	err = os.Chmod(dir, 0777)
	if err != nil {
		return "", err
	}

	err = os.Chmod(f.Name(), 0666)
	if err != nil {
		return "", err
	}

	return f.Name(), nil
}
