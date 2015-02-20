package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"runtime"
	"time"

	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
)

const (
	pubkey string = `-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.4.13 (GNU/Linux)

mQENBFTdUNwBCACdZSiKNBwUWsEfBBqN4JGuKBx7n9kQkD8sTn/i/InIe4GwxAs8
R9/A07GNJe4WSkVA52T248MYzpCVjRAgM+u2OZHj8Y036dR5VoSIvtwy/OApXdU8
SqzqyKpMK3w5cq5WP+ZNAZURYJbmdsZSW4aoT2YbqdHEAbudRWCOwI07f5wyJzrp
dLjSR4pXmBZ5P9vB4vS5uvYRFuqud9Bg/+Z/EY3uDVm2gb+/yH+iwhjKQcEpdxF/
yd7jom3DDHoOWtM/n+WO8V+iz7+fwRAhU+F9Dn1Ksx3cZuMYkPWHeSIzAg/5wYkk
zZjBgsjZw/tGvc3f/yw5UnWHD8jS18I1I4B5ABEBAAG0JHAydW5pdmVyc2UgPGRl
cGxveS1kZXZAc3F1YXJldXAuY29tPokBOAQTAQIAIgUCVN1Q3AIbAwYLCQgHAwIG
FQgCCQoLBBYCAwECHgECF4AACgkQs6P0At4Z1foIOwf/f4JjQR/fmq/4sA4RIQz/
qYOYpoBwA6VgFzlOsAjiCqE/xESmKofLVQAI2U9/aTJmzyVkvAA9C/UwWnXw45qM
1z+/4ZFAVivvte7IAdXdvUdaa0mL092yWDivZBbymlIjTv2r8/pC6Zzxh59pPYmt
1RoITo41l9YoZsmHFl5g4RT8j3vBV4i8q8RzF2pNhEPOK02qOnLOtoqGcVDQ/sXT
CJLyewDF6OtkbrDVcT7Fru5gqgE+yx+6T4wYGySMNC8lhj0JYrYAuWWAAl9OgPfb
L/8P1tsUANWG3DAB6921csJhXarX2zj+Iqap7O1VnATZMpUejiKAUvFDQW2PCxLv
QLkBDQRU3VDcAQgAwqGSw1+BLcn/te1Dafda/Ahps+KVOC9nsec+q7+Ie2V4X6g+
Qz0M4idKZbZVg+CSeWi9CFJ25Ti2IzqhBRxh0MM7j4yvr7vy6hREl63BVTQRv3nY
KsmSJ2UhOWRx/QgeiIrGCuQwlaBulk+sFJ/nIv4XkebpZ4UkM0dL6Hx6O0ljQ9eV
dbrRr7nPAu6YKR6TzESAxIy48iC/6GdYmgFHYJTRrRW6S9N4WnVd2FjLIoRDiYnD
41G6P6jLILUfm8OGj/QqR6KQZRBEIm3c5qM5VI/AwoE1wEANo8eCro9DmwofIFMh
AhJP3+QxbNETzrpC5dEQj+odn8OD+IkAUDeRPQARAQABiQEfBBgBAgAJBQJU3VDc
AhsMAAoJELOj9ALeGdX6UCMH/iUmZqpJcyBi+4f7+s6SIT8SKh9IwM/klkxVoOsK
S4eNHQK+0JXSTGLEx9XxNpSAZSke50fGcmzxokpb+IADH+p3ck3ZMu08395XzEMl
4w3cmUu6pdN7urNVQBrdWwxyZ66i2Dh/Vd83QmZ5rwk6m5x0Ob97JsJ9KO/Dvqic
JtI7nqAppVaYI9JeeOdSqjtddoQmuxzy4qfjT8M+D4y9/4ZcQGXFiKC5HOMaaar6
25MG3OVT1k329oT9beauOdo3RqW8SjK/6y0Sy1ZfuZodCO43Ak7gYisYT1BgDsyx
xisJuc75Ar7leHrw+7hdBeptVCVEaT9yylSTuwxugikXBzI=
=XSs4
-----END PGP PUBLIC KEY BLOCK-----`
	privkey string = `-----BEGIN PGP PRIVATE KEY BLOCK-----
Version: GnuPG v1.4.13 (GNU/Linux)

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
=I1Jf
-----END PGP PRIVATE KEY BLOCK-----`
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

	err = loadGPG(tempdir)
	if err != nil {
		log.Fatalf("Could not load GPG keys: %s\n", err)
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
	// _, err = user.CreateUser("hello", pods.PodPath("hello"))
	// if err != nil && err != user.AlreadyExists {
	// 	log.Fatalf("Could not create user: %s", err)
	// }
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

func loadGPG(workdir string) error {
	importpub := exec.Command("gpg", "--no-default-keyring", "--keyring", path.Join(workdir, "pubring.gpg"), "--secret-keyring", path.Join(workdir, "secring.gpg"), "--import")
	importpub.Stdin = bytes.NewBufferString(pubkey)
	err := importpub.Run()
	if err != nil {
		return util.Errorf("Couldn't load GPG public key: %s", err)
	}

	importpriv := exec.Command("gpg", "--no-default-keyring", "--keyring", path.Join(workdir, "pubring.gpg"), "--secret-keyring", path.Join(workdir, "secring.gpg"), "--allow-secret-key-import", "--import")
	importpriv.Stdin = bytes.NewBufferString(privkey)
	err = importpriv.Run()
	if err != nil {
		return util.Errorf("Couldn't load GPG private key: %s", err)
	}

	return nil
}

func signManifest(manifestPath string, workdir string) (string, error) {
	signedManifestPath := fmt.Sprintf("%s.asc", manifestPath)
	return signedManifestPath, exec.Command("gpg", "--no-default-keyring", "--keyring", path.Join(workdir, "pubring.gpg"), "--secret-keyring", path.Join(workdir, "secring.gpg"), "-u", "p2universe", "--output", signedManifestPath, "--clearsign", manifestPath).Run()
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

	manifest, err := pods.PodManifestFromPath(manifestPath)
	if err != nil {
		return "", err
	}
	manifest.Config["preparer"] = map[string]interface{}{
		"keyring": path.Join(workdir, "pubring.gpg"),
	}
	f, err := os.OpenFile(manifestPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	defer f.Close()
	err = manifest.Write(f)
	if err != nil {
		return "", err
	}

	return manifestPath, err
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
	defer f.Close()
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
	bootstr := exec.Command("p2-bootstrap", "--consul-pod", consulManifest, "--agent-pod", preparerManifest)
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
		helloT := exec.Command("tail", "/var/service/hello__hello/log/main/current")
		helloT.Stdout = &helloTail
		helloT.Run()
		preparerT := exec.Command("tail", "/var/service/preparer__preparer/log/main/current")
		preparerT.Stdout = &preparerTail
		preparerT.Run()
		return fmt.Errorf("Couldn't start hello after 15 seconds: \n\n hello tail: \n%s\n\n preparer tail: \n%s", helloTail.String(), preparerTail.String())
	case <-helloPidAppeared:
		return nil
	}
}
