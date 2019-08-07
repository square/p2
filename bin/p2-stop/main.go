package main

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	"github.com/sirupsen/logrus"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/constants"
	"github.com/square/p2/pkg/p2stop"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	fullDescription = `Safely disable and stop an existing pod.

EXAMPLES

$ p2-stop mypod

$ p2-stop --pod-dir /custom/pod/home

`
)

var (
	app = kingpin.New("p2-stop", fullDescription)

	nodeName = app.Flag("node-name", "The name of this node (default: hostname)").String()
	podDir   = app.Flag("pod-dir", "The directory where the pod to be halted is located. ").String()
	podName  = app.Arg("pod-name", fmt.Sprintf("The name of the pod to be halted. Looks in the default pod home '%s' for the pod", pods.DefaultPath)).String()
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

	if p2stop.PolicyPath == "" || p2stop.Keyring == "" || p2stop.DefaultDomain == "" {
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
		p2stop.Keyring,
		p2stop.PolicyPath,
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
	email := fmt.Sprintf("%s%s", u.Username, p2stop.DefaultDomain)
	if !authPolicy.Authorize(email, manifest.ID().String()) {
		logger.Fatalln("Could not authorize user, may not have permission to run script")
	}

	logger = logger.SubLogger(logrus.Fields{"pod": pod.Id})
	logger.NoFields().Infoln("Finding services to halt")

	ls, err := pod.Launchables(manifest)
	if err != nil {
		logger.WithError(err).Fatalln("error configuring user auth")
	}

	if len(ls) == 0 {
		logger.NoFields().Fatalln("No launchables in pod")
	}

	logger.NoFields().Infoln("Halting pod")

	// An operator is stopping the pod, don't allow the manifest to decide not to stop a launchable
	forceHalt := true
	ok, err := pod.Halt(manifest, forceHalt)
	if err != nil {
		logger.WithError(err).Fatalln("Could not halt pod")
	} else if !ok {
		logger.NoFields().Warningln("Failed to cleanly halt services. Some may have been sent a SIGKILL")
	} else {
		logger.NoFields().Infoln("Halt successful")
	}
}
