package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	restart = kingpin.New("p2-restart", `Safely disable, stop, start and enable an existing pod.

EXAMPLES

$ p2-restart mypod

$ p2-restart --pod-dir /custom/pod/home

`)

	nodeName = restart.Flag("node-name", "The name of this node (default: hostname)").String()
	podDir   = restart.Flag("pod-dir", "The directory where the pod to be restarted is located. ").String()
	podName  = restart.Arg("pod-name", fmt.Sprintf("The name of the pod to be restarted. Looks in the default pod home '%s' for the pod", pods.DefaultPath)).String()
)

func main() {
	restart.Version(version.VERSION)
	kingpin.MustParse(restart.Parse(os.Args[1:]))

	pods.Log.Logger.Formatter = &logrus.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
		TimestampFormat:  "15:04:05.000",
	}
	logger := pods.Log

	if *nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			logger.WithError(err).Fatal("Error getting node name")
		}
		*nodeName = hostname
	}

	if *podName == "" && *podDir == "" {
		logger.NoFields().Fatalln("Must pass a pod name or pod home directory")
	}

	var path string
	if *podName != "" {
		path = filepath.Join(pods.DefaultPath, *podName)
	} else {
		path = *podDir
	}

	// TODO: pass requirefile from old pod to new pod
	pod, err := pods.PodFromPodHome(types.NodeName(*nodeName), path)
	if err != nil {
		logger.NoFields().Fatalln(err)
	}

	manifest, err := pod.CurrentManifest()
	if err != nil {
		logger.NoFields().Fatalln(err)
	}

	logger = logger.SubLogger(logrus.Fields{"pod": pod.Id})
	logger.NoFields().Infoln("Finding services to restart")

	services, err := pod.Services(manifest)
	if err != nil {
		logger.WithError(err).Fatalln("Could not determine services to restart")
	}

	ls, err := pod.Launchables(manifest)
	if err != nil {
		logger.WithError(err).Fatalln("Could not determine launchables in pod")
	}

	if len(ls) == 0 {
		logger.NoFields().Fatalln("No launchables in pod")
	}

	for _, service := range services {
		res, err := runit.DefaultSV.Stat(&service)
		if err != nil {
			logger.WithErrorAndFields(err, logrus.Fields{"service": service.Name}).Fatalln("Could not stat service")
		}
		logger.WithFields(logrus.Fields{
			"service": service.Name,
			"time":    res.ChildTime,
			"status":  res.ChildStatus,
			"PID":     res.ChildPID,
		}).Infoln("Will restart")
	}

	logger.NoFields().Infoln("Halting pod")

	// let the pod manifest decide which launchables to actually stop
	force := false
	ok, err := pod.Halt(manifest, force)
	if err != nil {
		logger.WithError(err).Fatalln("Could not halt pod")
	} else if !ok {
		logger.NoFields().Warningln("Failed to cleanly halt services. Some may have been sent a SIGKILL.")
	}

	logger.NoFields().Infoln("Starting pod")

	ok, err = pod.Launch(manifest)
	if err != nil {
		logger.WithError(err).Fatalln("Could not start pod")
	} else if !ok {
		logger.NoFields().Warningln("Some services did not come up - check output for details")
	}

	logger.NoFields().Infoln("Restart successful.")
}
