package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"runtime"
	"time"

	"github.com/square/p2/pkg/kv-consul"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

func main() {
	// 1. Generate pod for preparer in this code version (`rake artifact:prepare`)
	// 2. Locate manifests for preparer pod, premade consul pod
	// 3. Execute bootstrap with premade consul pod and preparer pod
	// 4. Deploy hello pod manifest by pushing to intent store
	// 5. Verify that hello is running (listen to syslog? verify Runit PIDs? Both?)
	tempdir, err := ioutil.TempDir("", "single-node-check")
	log.Printf("Putting test manifests in %s\n", tempdir)
	if err != nil {
		log.Fatalln("Could not create temp directory, bailing")
	}
	preparerManifest, err := generatePreparerPod(tempdir)
	if err != nil {
		log.Fatalf("Could not generate preparer pod: %s\n", err)
	}
	consulManifest, err := getConsulManifest(tempdir)
	if err != nil {
		log.Fatalf("Could not generate consul pod: %s\n", err)
	}
	fmt.Println("Executing bootstrap")
	err = executeBootstrap(preparerManifest, consulManifest)
	if err != nil {
		log.Fatalf("Could not execute bootstrap: %s", err)
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

func generatePreparerPod(dir string) (string, error) {
	// build the artifact from HEAD
	cmd := exec.Command("rake", "artifact:preparer")
	err := cmd.Run()
	if err != nil {
		return "", err
	}
	candidateTars, err := util.From(runtime.Caller(0)).Glob("../../target/preparer_*.tar.gz")
	if err != nil {
		return "", util.Errorf("Failed to glob a preparer tar: %s", err)
	}
	if len(candidateTars) != 1 {
		return "", util.Errorf("Ambiguous preparer tar, remove them")
	}
	preparerTar := fmt.Sprintf("file://%s", candidateTars[0])
	manifest := &pods.PodManifest{}
	manifest.Id = "preparer"
	stanza := pods.LaunchableStanza{
		LaunchableId:   "preparer",
		LaunchableType: "hoist",
		Location:       preparerTar,
	}
	manifest.LaunchableStanzas = map[string]pods.LaunchableStanza{
		"preparer": stanza,
	}
	manifest.Config = make(map[string]interface{})
	manifest.Config["node_name"] = "testnode"
	preparerPath := path.Join(dir, "preparer.yaml")
	f, err := os.OpenFile(preparerPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	err = manifest.Write(f)
	if err != nil {
		return "", err
	}
	return preparerPath, f.Close()
}

func getConsulManifest(dir string) (string, error) {
	consulTar := fmt.Sprintf("file://%s", util.From(runtime.Caller(0)).ExpandPath("../hoisted-consul_abc123.tar.gz"))
	manifest := &pods.PodManifest{}
	manifest.Id = "intent"
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
	err = manifest.Write(f)
	if err != nil {
		return "", err
	}
	return consulPath, f.Close()
}

func executeBootstrap(preparerManifest, consulManifest string) error {
	cmd := exec.Command("rake", "install")
	cmd.Stderr = os.Stdout
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("Could not install newest bootstrap: %s", err)
	}
	bootstr := exec.Command("bootstrap", "--consul-pod", consulManifest, "--agent-pod", preparerManifest)
	bootstr.Stdout = os.Stdout
	bootstr.Stderr = os.Stdout
	return bootstr.Run()
}

func postHelloManifest(dir string) error {
	hello := fmt.Sprintf("file://%s", util.From(runtime.Caller(0)).ExpandPath("../hoisted-hello_def456.tar.gz"))
	manifest := &pods.PodManifest{}
	manifest.Id = "hello"
	stanza := pods.LaunchableStanza{
		LaunchableId:   "hello",
		LaunchableType: "hoist",
		Location:       hello,
	}
	manifest.LaunchableStanzas = map[string]pods.LaunchableStanza{
		"hello": stanza,
	}
	client, err := ppkv.NewClient()
	if err != nil {
		return err
	}
	buf := bytes.Buffer{}
	err = manifest.Write(&buf)
	if err != nil {
		return err
	}

	return client.Put("nodes/testnode/hello", buf.String())
}

func verifyHelloRunning() error {
	helloPidAppeared := make(chan struct{})
	quit := make(chan struct{})
	defer close(quit)
	go func() {
		for {
			time.Sleep(100 * time.Millisecond)
			res := exec.Command("sudo", "sv", "stat", "/var/service/hello__hello").Run()
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
	case <-time.After(10 * time.Second):
		return fmt.Errorf("Couldn't start hello after 10 seconds")
	case <-helloPidAppeared:
		return nil
	}
}
