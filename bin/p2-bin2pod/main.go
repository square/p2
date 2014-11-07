package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"

	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/uri"
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
    cat example.json | jq '.["manifest_path"] | xargs -I {} curl -X PUT https://consul.dev:8500/api/v1/kv/nodes/$(hostname)/example -d {}
    `)
	executable = bin2pod.Arg("executable", "the executable to turn into a hoist artifact + pod manifest. The format of executable is of a URL. If referencing a path, use file:// as a prefix.").String()
	id         = bin2pod.Flag("id", "The ID of the pod. By default this is the name of the executable passed").String()
	location   = bin2pod.Flag("location", "The location where the outputted tar will live. The characters {} will be replaced with the unique basename of the tar, including its SHA. If not provided, the location will be a file path to the resulting tar from the build, which is included in the output of this script. Users must copy the resultant tar to the new location if it is different from the default output path.").String()
)

type output struct {
	TarPath       string `json:"tar_path"`
	ManifestPath  string `json:"manifest_path"`
	FinalLocation string `json:"final_location"`
}

func podId() string {
	if *id != "" {
		return *id
	}
	return *executable
}

func main() {
	bin2pod.Parse(os.Args[1:])

	res := output{}
	manifest := &pods.PodManifest{}
	manifest.Id = podId()
	manifest.LaunchableStanzas = map[string]pods.LaunchableStanza{}
	stanza := pods.LaunchableStanza{}
	stanza.LaunchableId = podId()
	stanza.LaunchableType = "hoist"

	workingDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Couldn't get the current working directory: %s", err)
	}

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

func makeTar(workingDir string, manifest *pods.PodManifest) (string, error) {
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
	err = uri.CopyFile(launchablePath, *executable)
	if err != nil {
		return "", fmt.Errorf("Couldn't copy from %s.: %s", *executable, err)
	}
	sha, err := manifest.SHA()
	if err != nil {
		return "", err
	}
	tarPath := path.Join(workingDir, fmt.Sprintf("%s_%s.tar.gz", path.Base(*executable), sha))
	cmd := exec.Command("tar", "-czvf", tarPath, "-C", tarContents, ".")
	err = cmd.Run()
	if err != nil {
		return "", fmt.Errorf("Couldn't build tar: %s", err)
	}
	return tarPath, nil
}

func writeManifest(workingDir string, manifest *pods.PodManifest) (string, error) {
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
