package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v1"
)

var (
	bin2pod = kingpin.New("bin2pod", `Convert an executable file into a pod manifest with a single hoist-type launchable.
    EXAMPLES

    1. Simple example
    -----------------

    $ p2-bin2pod example.sh
    {
      "tar_path": "/Users/joe/example.sh_63754b9a17f9479bdd90a510d3eee6635a6060a8.tar.gz",
      "manifest_path": "/Users/joe/example.sh.yaml",
      "final_location": "/Users/joe/example.sh_63754b9a17f9479bdd90a510d3eee6635a6060a8.tar.gz"
    }

    2. Copy to fileserver and intent store.
    ---------------------------------------

    p2-bin2pod --location http://localhost:5000/{} example.sh > example.json
    cat example.json | jq '.["tar_path"] | xargs -I {} cp {} ~/test_file_server_dir
    cat example.json | jq '.["manifest_path"] | xargs -I {} curl -X PUT https://consul.dev:8500/api/v1/kv/intent/$(hostname)/example -d {}
    `)
	executable    = bin2pod.Arg("executable", "the executable to turn into a hoist artifact + pod manifest. The format of executable is of a URL.").Required().String()
	id            = bin2pod.Flag("id", "The ID of the pod. By default this is the name of the executable passed").String()
	location      = bin2pod.Flag("location", "The location where the outputted tar will live. The characters {} will be replaced with the unique basename of the tar, including its SHA. If not provided, the location will be a file path to the resulting tar from the build, which is included in the output of this script. Users must copy the resultant tar to the new location if it is different from the default output path.").String()
	workDirectory = bin2pod.Flag("work-dir", "A directory where the results will be written.").ExistingDir()
	config        = bin2pod.Flag("config", "a list of key=value assignments. Each key will be set in the config section.").Strings()
)

type Result struct {
	TarPath       string `json:"tar_path"`
	ManifestPath  string `json:"manifest_path"`
	FinalLocation string `json:"final_location"`
}

func podId() string {
	if *id != "" {
		return *id
	}
	return path.Base(*executable)
}

func activeDir() string {
	if *workDirectory != "" {
		return *workDirectory
	}
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Cannot get current directory: %s", err)
	}
	return dir
}

func main() {
	bin2pod.Version(version.VERSION)
	bin2pod.Parse(os.Args[1:])

	res := Result{}
	manifest := &pods.Manifest{}
	manifest.Id = podId()
	manifest.LaunchableStanzas = map[string]pods.LaunchableStanza{}
	stanza := pods.LaunchableStanza{}
	stanza.LaunchableId = podId()
	stanza.LaunchableType = "hoist"

	workingDir := activeDir()

	err := addManifestConfig(manifest)
	tarLocation, err := makeTar(workingDir, manifest)
	if err != nil {
		log.Fatalln(err.Error())
	}
	res.TarPath = tarLocation

	if *location != "" {
		stanza.Location = *location
		res.FinalLocation = *location
	} else {
		stanza.Location = tarLocation
		res.FinalLocation = tarLocation
	}
	manifest.LaunchableStanzas[podId()] = stanza

	if err != nil {
		log.Fatalln(err.Error())
	}

	res.ManifestPath, err = writeManifest(workingDir, manifest)
	if err != nil {
		log.Fatalf("Couldn't write manifest: %s", err)
	}

	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		log.Fatalf("Couldn't marshal output: %s", err)
	}
	_, err = os.Stdout.Write(b)
	if err != nil {
		log.Fatalf("Couldn't write to stdout: %s", err)
	}
}

func makeTar(workingDir string, manifest *pods.Manifest) (string, error) {
	tarContents := path.Join(workingDir, fmt.Sprintf("%s.workd", podId()))
	err := os.MkdirAll(tarContents, 0744)
	defer os.RemoveAll(tarContents)
	if err != nil {
		return "", fmt.Errorf("Couldn't make a new working directory %s for tarring: %s", tarContents, err)
	}
	err = os.MkdirAll(path.Join(tarContents, "bin"), 0744)
	if err != nil {
		return "", fmt.Errorf("Couldn't make bin directory in %s: %s", tarContents, err)
	}
	launchablePath := path.Join(tarContents, "bin", "launch")
	err = uri.URICopy(*executable, launchablePath)
	if err != nil {
		return "", fmt.Errorf("Couldn't copy from %s.: %s", *executable, err)
	}
	err = os.Chmod(launchablePath, 0755) // make file executable by all.
	if err != nil {
		return "", fmt.Errorf("Couldn't make %s executable: %s", launchablePath, err)
	}
	tarPath := path.Join(workingDir, fmt.Sprintf("%s_%s.tar.gz", path.Base(*executable), randomSuffix()))
	cmd := exec.Command("tar", "-czvf", tarPath, "-C", tarContents, ".")
	err = cmd.Run()
	if err != nil {
		return "", fmt.Errorf("Couldn't build tar: %s", err)
	}
	return tarPath, nil
}

func addManifestConfig(manifest *pods.Manifest) error {
	manifest.Config = make(map[interface{}]interface{})
	for _, pair := range *config {
		res := strings.Split(pair, "=")
		if len(res) != 2 {
			return fmt.Errorf("%s is not a valid key=value config assignment", pair)
		}
		manifest.Config[res[0]] = res[1]
	}
	return nil
}

func writeManifest(workingDir string, manifest *pods.Manifest) (string, error) {
	file, err := os.OpenFile(path.Join(workingDir, fmt.Sprintf("%s.yaml", podId())), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	defer file.Close()
	if err != nil {
		return "", err
	}

	err = manifest.Write(file)
	if err != nil {
		return "", err
	}
	return file.Name(), nil
}

func randomSuffix() string {
	rand.Seed(time.Now().UnixNano())
	set := []rune("ghijklmnopqrstuvwxyz")
	strlen := 40
	res := make([]string, strlen)
	for i := 0; i < strlen; i++ {
		res[i] = string(set[rand.Intn(len(set))])
	}
	return strings.Join(res, "")
}
