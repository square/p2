package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/flags"
	"github.com/square/p2/pkg/version"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const (
	cmdFetchText = "fetch"
)

var (
	cmdFetch     = kingpin.Command(cmdFetchText, "Fetch the manifest for a given pod from the datastore and write it to a file on the disk. Combines well with p2-launch.")
	fetchTree    = cmdFetch.Flag("tree", "Intent or Reality").Default("intent").Short('t').Enum("intent", "reality")
	fetchPod     = cmdFetch.Flag("podID", "The pod ID for which to fetch its manifest").Short('i').Required().String()
	fetchEntropy = cmdFetch.Flag("force-restart", "Introduce entropy into the manifest before outputting it. Scheduling the resulting manifest will definitely cause a restart of the pod.").Short('p').Bool()
)

func main() {
	kingpin.Version(version.VERSION)
	logger := logging.NewLogger(logrus.Fields{})
	cmd, consulOpts, labeler := flags.ParseWithConsulOptions()
	_ = labeler
	client := consul.NewConsulClient(consulOpts)

	switch cmd {
	case cmdFetchText:
		hostname, err := os.Hostname()
		if err != nil {
			logger.Fatal(err)
		}
		man, err := fetch(client, *fetchTree, hostname, *fetchPod, *fetchEntropy)
		if err != nil {
			logger.Fatal(err)
		}
		filename := fmt.Sprintf("%s.yaml.signed", *fetchPod)
		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			logger.Fatalf("Could not open file at %s. %v", filename, err)
		}

		err = man.Write(file)
		if err != nil {
			logger.Fatalf("Unable to write manifest: %v", err)
		}
		fmt.Printf("%s", file.Name())
	default:
		logger.Fatalln("Did not receive a command. Try --help.")
	}
}

func fetch(client consulutil.ConsulClient, tree, hostname, pod string, entropy bool) (manifest.Manifest, error) {
	key := fmt.Sprintf("%s/%s/%s", tree, hostname, pod)
	resp, _, err := client.KV().Get(key, nil)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errors.New("Got nil response from the datastore")
	}

	man, err := manifest.FromBytes(resp.Value)
	if err != nil {
		return nil, err
	}

	if entropy {
		man, err = introduceEntropy(man)
		if err != nil {
			return nil, err
		}
	}

	return man, nil
}

func introduceEntropy(man manifest.Manifest) (manifest.Manifest, error) {
	mb := man.GetBuilder()
	config := man.GetConfig()
	config["_restart"] = fmt.Sprintf("restarted at: %s", time.Now().Format(time.UnixDate))
	err := mb.SetConfig(config)
	if err != nil {
		return nil, err
	}
	return mb.GetManifest(), nil
}
