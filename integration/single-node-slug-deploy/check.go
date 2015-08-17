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
	"time"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/util"
)

const preparerStatusPort = 32170
const certpath = "/var/tmp/certs"

func main() {
	// 1. Generate pod for preparer in this code version (`rake artifact:prepare`)
	// 2. Locate manifests for preparer pod, premade consul pod
	// 3. Execute bootstrap with premade consul pod and preparer pod
	// 4. Deploy hello pod manifest by pushing to intent store
	// 5. Verify that hello is running (listen to syslog? verify Runit PIDs? Both?)

	// list of services running on integration test host
	//services := []string{"p2-preparer", "hello"}
	tempdir, err := ioutil.TempDir("", "single-node-check")
	log.Printf("Putting test manifests in %s\n", tempdir)
	if err != nil {
		log.Fatalln("Could not create temp directory, bailing")
	}
	preparerManifest, err := generatePreparerPod(tempdir)
	if err != nil {
		log.Fatalf("Could not generate preparer pod: %s\n", err)
	}
	_, err = preparer.LoadConfig(preparerManifest)
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
	err = postHelloManifest(tempdir)
	if err != nil {
		log.Fatalf("Could not generate hello pod: %s\n", err)
	}
	err = verifyHelloRunning()
	if err != nil {
		log.Fatalf("Couldn't get hello running: %s", err)
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
	manifestPath, err := executeBin2Pod(cmd)
	if err != nil {
		return "", err
	}

	manifest, err := pods.ManifestFromPath(manifestPath)
	if err != nil {
		return "", err
	}
	manifest.Config["preparer"] = map[string]interface{}{
		"auth": map[string]string{
			"type":    "keyring",
			"keyring": util.From(runtime.Caller(0)).ExpandPath("pubring.gpg"),
		},
		"ca_file":     filepath.Join(certpath, "cert.pem"),
		"cert_file":   filepath.Join(certpath, "cert.pem"),
		"key_file":    filepath.Join(certpath, "key.pem"),
		"status_port": preparerStatusPort,
	}
	manifest.RunAs = "root"
	manifest.StatusPort = preparerStatusPort
	manifest.StatusHTTP = true

	manifestBytes, err := manifest.Marshal()
	if err != nil {
		return "", err
	}

	err = ioutil.WriteFile(manifestPath, manifestBytes, 0644)
	if err != nil {
		return "", err
	}

	return manifestPath, err
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
	manifestPath, err := executeBin2Pod(cmd)
	if err != nil {
		return err
	}

	userHookManifest, err := pods.ManifestFromPath(manifestPath)
	if err != nil {
		return err
	}

	userHookManifest.RunAs = "root"
	contents, err := userHookManifest.Marshal()
	if err != nil {
		return err
	}

	ioutil.WriteFile(manifestPath, contents, 0644)

	manifestPath, err = signManifest(manifestPath, tmpdir)
	if err != nil {
		return err
	}
	return exec.Command("p2-schedule", "--hook-type", "before_install", manifestPath).Run()
}

func executeBin2Pod(cmd *exec.Cmd) (string, error) {
	out := bytes.Buffer{}
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return "", util.Errorf("p2-bin2pod failed: %s", err)
	}
	var bin2podres map[string]string
	err = json.Unmarshal(out.Bytes(), &bin2podres)
	if err != nil {
		return "", err
	}
	return bin2podres["manifest_path"], nil
}

func getConsulManifest(dir string) (string, error) {
	consulTar := fmt.Sprintf("file://%s", util.From(runtime.Caller(0)).ExpandPath("../hoisted-consul_abc123.tar.gz"))
	manifest := &pods.Manifest{}
	manifest.Id = "consul"
	stanza := pods.LaunchableStanza{
		LaunchableId:   "consul",
		LaunchableType: "hoist",
		Location:       consulTar,
	}
	manifest.LaunchableStanzas = map[string]pods.LaunchableStanza{
		"consul": stanza,
	}
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

func postHelloManifest(dir string) error {
	hello := fmt.Sprintf("file://%s", util.From(runtime.Caller(0)).ExpandPath("../hoisted-hello_def456.tar.gz"))
	manifest := &pods.Manifest{}
	manifest.Id = "hello"
	manifest.StatusPort = 43770
	stanza := pods.LaunchableStanza{
		LaunchableId:   "hello",
		LaunchableType: "hoist",
		Location:       hello,
	}
	manifest.LaunchableStanzas = map[string]pods.LaunchableStanza{
		"hello": stanza,
	}
	manifestPath := path.Join(dir, "hello.yaml")

	f, err := os.OpenFile(manifestPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	err = manifest.Write(f)
	if err != nil {
		return err
	}
	f.Close()

	manifestPath, err = signManifest(manifestPath, dir)
	if err != nil {
		return err
	}

	return exec.Command("p2-schedule", manifestPath).Run()
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
		var helloTail, preparerTail bytes.Buffer
		helloT := exec.Command("tail", "/var/service/hello__hello__launch/log/main/current")
		helloT.Stdout = &helloTail
		helloT.Run()
		preparerT := exec.Command("tail", "/var/service/p2-preparer__p2-preparer__launch/log/main/current")
		preparerT.Stdout = &preparerTail
		preparerT.Run()
		return fmt.Errorf("Couldn't start hello after 15 seconds: \n\n hello tail: \n%s\n\n preparer tail: \n%s", helloTail.String(), preparerTail.String())
	case <-helloPidAppeared:
		return nil
	}
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
	for _, sv := range services {
		res, err := store.GetHealth(sv, name)
		if err != nil {
			return err
		} else if (res == kp.WatchResult{}) {
			return fmt.Errorf("No results for %s", sv)
		} else if res.Status != string(health.Passing) {
			return fmt.Errorf("%s did not pass health check", sv)
		} else {
			fmt.Println(res)
		}
	}

	for _, sv := range services {
		res, err := store.GetServiceHealth(sv)
		getres, _ := store.GetHealth(sv, name)
		if err != nil {
			return err
		}
		val := res[kp.HealthPath(sv, name)]
		if getres.Id != val.Id || getres.Service != val.Service || getres.Status != val.Status {
			return fmt.Errorf("GetServiceHealth failed %+v", res)
		}
	}

	// if it reaches here it means health checks
	// are being written to the KV store properly
	return nil
}
