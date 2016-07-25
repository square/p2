package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/rc"
	"github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	klabels "k8s.io/kubernetes/pkg/labels"
)

const preparerStatusPort = 32170
const certpath = "/var/tmp/certs"

func main() {
	// 1. Generate pod for preparer in this code version (`rake artifact:prepare`)
	// 2. Locate manifests for preparer pod, premade consul pod
	// 3. Execute bootstrap with premade consul pod and preparer pod
	// 4. Deploy p2-rctl-server pod with p2-schedule
	// 5. Schedule a hello pod manifest with a replication controller
	// 6. Verify that p2-rctl-server is running by checking health.
	// 7. Verify that hello is running by checking health. Monitor using
	// written pod label queries.

	// list of services running on integration test host
	services := []string{"p2-preparer", "hello"}
	tempdir, err := ioutil.TempDir("", "single-node-check")
	log.Printf("Putting test manifests in %s\n", tempdir)
	if err != nil {
		log.Fatalln("Could not create temp directory, bailing")
	}
	preparerManifest, err := generatePreparerPod(tempdir)
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
	err = executeBootstrap(signedPreparerManifest, signedConsulManifest)
	if err != nil {
		log.Fatalf("Could not execute bootstrap: %s", err)
	}

	// Wait a bit for preparer's http server to be ready
	err = waitForStatus(preparerStatusPort, "preparer", 10*time.Second)
	if err != nil {
		log.Fatalf("Couldn't check preparer status: %s", err)
	}

	err = scheduleUserCreationHook(tempdir)
	if err != nil {
		log.Fatalf("Couldn't schedule the user creation hook: %s", err)
	}
	err = scheduleRCTLServer(tempdir)
	if err != nil {
		log.Fatalf("Could not schedule RCTL server: %s", err)
	}
	rcID, err := createHelloReplicationController(tempdir)
	if err != nil {
		log.Fatalf("Could not create hello pod / rc: %s\n", err)
	}
	log.Printf("Created RC #%s for hello\n", rcID)

	err = waitForPodLabeledWithRC(klabels.Everything().Add(rc.RCIDLabel, klabels.EqualsOperator, []string{rcID.String()}), rcID)
	if err != nil {
		log.Fatalf("Failed waiting for pods labeled with the given RC: %v", err)
	}
	err = verifyHelloRunning()
	if err != nil {
		log.Fatalf("Couldn't get hello running: %s", err)
	}
	err = verifyHealthChecks(config, services)
	if err != nil {
		log.Fatalf("Could not get health check info from consul: %s", err)
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
	return exec.Command("gpg", "--no-default-keyring",
		"--keyring", util.From(runtime.Caller(0)).ExpandPath("pubring.gpg"),
		"--secret-keyring", util.From(runtime.Caller(0)).ExpandPath("secring.gpg"),
		"-u", "p2universe",
		"--out", sigLoc,
		"--detach-sign", artifactPath).Run()
}

func generatePreparerPod(workdir string) (string, error) {
	// build the artifact from HEAD
	err := exec.Command("go", "build", "github.com/square/p2/bin/p2-preparer").Run()
	if err != nil {
		return "", util.Errorf("Couldn't build preparer: %s", err)
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
			if err != nil {
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

func scheduleUserCreationHook(tmpdir string) error {
	createUserPath := path.Join(tmpdir, "create_user")
	script := `#!/usr/bin/env bash
set -e
mkdir -p $HOOKED_POD_HOME
/sbin/adduser $HOOKED_POD_ID -d $HOOKED_POD_HOME
`
	err := ioutil.WriteFile(createUserPath, []byte(script), 0744)
	if err != nil {
		return err
	}

	cmd := exec.Command("p2-bin2pod", "--work-dir", tmpdir, createUserPath)
	createUserBin2Pod, err := executeBin2Pod(cmd)
	if err != nil {
		return err
	}

	if err = signBuild(createUserBin2Pod.TarPath); err != nil {
		return err
	}
	manifestPath := createUserBin2Pod.ManifestPath

	userHookManifest, err := manifest.FromPath(manifestPath)
	if err != nil {
		return err
	}

	builder := userHookManifest.GetBuilder()

	builder.SetRunAsUser("root")
	userHookManifest = builder.GetManifest()
	contents, err := userHookManifest.Marshal()
	if err != nil {
		return err
	}

	ioutil.WriteFile(manifestPath, contents, 0644)

	manifestPath, err = signManifest(manifestPath, tmpdir)
	if err != nil {
		return err
	}
	return exec.Command("p2-schedule", "--hook", manifestPath).Run()
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
	consulTar := fmt.Sprintf(
		"file://%s",
		util.From(runtime.Caller(0)).ExpandPath("../hoisted-consul_052.tar.gz"),
	)
	builder := manifest.NewBuilder()
	builder.SetID("consul")
	stanzas := map[launch.LaunchableID]launch.LaunchableStanza{
		"consul": {
			LaunchableId:   "consul",
			LaunchableType: "hoist",
			Location:       consulTar,
		},
	}
	builder.SetLaunchables(stanzas)
	manifest := builder.GetManifest()

	_ = signBuild(consulTar)

	consulPath := path.Join(dir, "consul.yaml")
	f, err := os.OpenFile(consulPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	defer f.Close()

	err = manifest.Write(f)
	if err != nil {
		return "", err
	}
	return consulPath, f.Close()
}

func executeBootstrap(preparerManifest, consulManifest string) error {
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
	bootstr := exec.Command("p2-bootstrap", "--consul-pod", consulManifest, "--agent-pod", preparerManifest)
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

func createHelloReplicationController(dir string) (fields.ID, error) {
	hello := fmt.Sprintf("file://%s", util.From(runtime.Caller(0)).ExpandPath("../hoisted-hello_def456.tar.gz"))
	builder := manifest.NewBuilder()
	builder.SetID("hello")
	builder.SetStatusPort(43770)
	stanzas := map[launch.LaunchableID]launch.LaunchableStanza{
		"hello": {
			LaunchableId:   "hello",
			LaunchableType: "hoist",
			Location:       hello,
		},
	}
	builder.SetLaunchables(stanzas)
	manifest := builder.GetManifest()
	manifestPath := path.Join(dir, "hello.yaml")

	f, err := os.OpenFile(manifestPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fields.ID(""), err
	}
	defer f.Close()
	err = manifest.Write(f)
	if err != nil {
		return fields.ID(""), err
	}
	f.Close()

	manifestPath, err = signManifest(manifestPath, dir)
	if err != nil {
		return fields.ID(""), err
	}

	cmd := exec.Command("p2-rctl", "--log-json", "create", "--manifest", manifestPath, "--node-selector", "test=yes")
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

	return fields.ID(rctlOut.ID), exec.Command("p2-rctl", "set-replicas", rctlOut.ID, "1").Run()
}

func waitForPodLabeledWithRC(selector klabels.Selector, rcID fields.ID) error {
	client := kp.NewConsulClient(kp.Options{})
	applicator := labels.NewConsulApplicator(client, 1)

	// we have to label this hostname as being allowed to run tests
	host, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("Could not get hostname: %s", err)
	}
	err = applicator.SetLabel(labels.NODE, host, "test", "yes")
	if err != nil {
		return fmt.Errorf("Could not set node selector label on %s: %v", host, err)
	}

	quitCh := make(chan struct{})
	defer close(quitCh)
	watchCh := applicator.WatchMatches(selector, labels.POD, quitCh)
	waitTime := time.After(30 * time.Second)
	for {
		select {
		case <-waitTime:
			return fmt.Errorf("Label selector %v wasn't matched before timeout: %s", selector, targetLogs())
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

func verifyHelloRunning() error {
	helloPidAppeared := make(chan struct{})
	quit := make(chan struct{})
	defer close(quit)
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			res := exec.Command("sudo", "sv", "stat", "/var/service/hello__hello__launch").Run()
			if res == nil {
				select {
				case <-quit:
					fmt.Println("got a valid stat after timeout")
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
	case <-time.After(20 * time.Second):
		return fmt.Errorf("Couldn't start hello after 15 seconds:\n\n %s", targetLogs())
	case <-helloPidAppeared:
		return nil
	}
}

func targetLogs() string {
	var helloTail, preparerTail bytes.Buffer
	helloT := exec.Command("tail", "/var/service/hello__hello__launch/log/main/current")
	helloT.Stdout = &helloTail
	helloT.Run()
	preparerT := exec.Command("tail", "/var/service/p2-preparer__p2-preparer__launch/log/main/current")
	preparerT.Stdout = &preparerTail
	preparerT.Run()
	return fmt.Sprintf("hello tail: \n%s\n\n preparer tail: \n%s", helloTail.String(), preparerTail.String())
}

func verifyHealthChecks(config *preparer.PreparerConfig, services []string) error {
	store, err := config.GetStore()
	if err != nil {
		return err
	}

	time.Sleep(5 * time.Second)
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
		} else if (res == kp.WatchResult{}) {
			return fmt.Errorf("No results for %s: \n\n %s", sv, targetLogs())
		} else if res.Status != string(health.Passing) {
			return fmt.Errorf("%s did not pass health check: \n\n %s", sv, targetLogs())
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
		val := res[kp.HealthPath(sv, node)]
		if getres.Id != val.Id || getres.Service != val.Service || getres.Status != val.Status {
			return fmt.Errorf("GetServiceHealth failed %+v: \n\n%s", res, targetLogs())
		}
	}

	// if it reaches here it means health checks
	// are being written to the KV store properly
	return nil
}
