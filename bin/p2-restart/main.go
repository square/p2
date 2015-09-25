package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v1"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/version"
)

var (
	restart = kingpin.New("p2-restart", `Safely disable, stop, start and enable an existing pod.

EXAMPLES

$ p2-restart mypod

$ p2-restart --pod-dir /custom/pod/home

`)

	podName = restart.Arg("pod-name", fmt.Sprintf("The name of the pod to be restarted. Looks in the default pod home '%s' for the pod", pods.DEFAULT_PATH)).String()
	podDir  = restart.Flag("pod-dir", "The directory where the pod to be restarted is located. ").String()
)

func main() {
	restart.Version(version.VERSION)
	restart.Parse(os.Args[1:])

	pods.Log.Logger.Formatter = &logrus.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
		TimestampFormat:  "15:04:05.000",
	}
	logger := pods.Log

	if *podName == "" && *podDir == "" {
		logger.NoFields().Fatalln("Must pass a pod name or pod home directory")
	}

	var path string
	if *podName != "" {
		path = filepath.Join(pods.DEFAULT_PATH, *podName)
	} else {
		path = *podDir
	}

	pod, err := pods.ExistingPod(path)
	if err != nil {
		logger.NoFields().Fatalln(err)
	}

	manifest, err := pod.CurrentManifest()
	if err != nil {
		logger.NoFields().Fatalln(err)
	}

	logger.WithField("pod", pod.Id).Infoln("Halting pod")

	ok, err := pod.Halt(manifest)
	if err != nil {
		logger.WithError(err).Fatalln("Could not halt pod")
	} else if !ok {
		logger.NoFields().Warningln("Had to forcibly kill some services")
	}

	logger.WithField("pod", pod.Id).Infoln("Starting pod")

	ok, err = pod.Launch(manifest)
	if err != nil {
		logger.WithError(err).Fatalln("Could not start pod")
	} else if !ok {
		logger.NoFields().Warningln("Some services did not come up quickly")
	}

	logger.WithField("pod", pod.Id).Infoln("Restart successful.")
}
