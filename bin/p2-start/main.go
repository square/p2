package main

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/constants"
	"github.com/square/p2/pkg/p2start"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	fullDescription = `Safely start and enable an existing pod.

EXAMPLES

$ p2-start mypod

$ p2-start --pod-dir /custom/pod/home

`
)

var (
	app = kingpin.New("p2-start", fullDescription)

	nodeName = app.Flag("node-name", "The name of this node (default: hostname)").String()
	podDir   = app.Flag("pod-dir", "The directory where the pod to be started is located. ").String()
	podName  = app.Arg("pod-name", fmt.Sprintf("The name of the pod to be started. Looks in the default pod home '%s' for the pod", pods.DefaultPath)).String()
)

func main() {
	app.Version(version.VERSION)
	kingpin.MustParse(app.Parse(os.Args[1:]))

	pods.Log.Logger.Formatter = &logrus.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
		TimestampFormat:  "15:04:05.000",
	}
	logger := pods.Log

	if p2start.PolicyPath == "" || p2start.Keyring == "" || p2start.DefaultDomain == "" {
		logger.Fatalln("PolicyPath or Keyring or DefaultDomain need to be set at build time")
	}

	if *nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			logger.WithError(err).Fatalln("Error getting node name")
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

	authPolicy, err := auth.NewUserPolicy(
		p2start.Keyring,
		p2start.PolicyPath,
		constants.PreparerPodID,
		constants.PreparerPodID.String(),
	)
	if err != nil {
		logger.WithError(err).Fatalln("error configuring user auth")
	}

	uid := strconv.Itoa(os.Getuid())
	u, err := user.LookupId(uid)
	if err != nil {
		logger.WithError(err).Fatalln("Could not lookup user")
	}
	if u.Username == "" {
		logger.Fatalln("Could not get Username from lookup")
	}
	email := fmt.Sprintf("%s%s", u.Username, p2start.DefaultDomain)
	if !authPolicy.Authorize(email, manifest.ID().String()) {
		logger.WithError(err).Fatalln("Could not authorize user, may not have permission to run script")
	}

	logger = logger.SubLogger(logrus.Fields{"pod": pod.Id})
	logger.NoFields().Infoln("Finding services to start")

	ls, err := pod.Launchables(manifest)
	if err != nil {
		logger.WithError(err).Fatalln("Could not determine launchables in pod")
	}

	if len(ls) == 0 {
		logger.NoFields().Fatalln("No launchables in pod")
	}

	logger.NoFields().Infoln("Starting pod")

	ok, err := pod.Launch(manifest)
	if err != nil {
		logger.WithError(err).Fatalln("Could not start pod")
	} else if !ok {
		logger.NoFields().Warningln("Some services did not come up - check output for details")
	} else {
		logger.NoFields().Infoln("Start successful")
	}
}
