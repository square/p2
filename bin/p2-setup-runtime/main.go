package main

import (
	"os"

	"github.com/sirupsen/logrus"

	dockerclient "github.com/docker/docker/client"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	fullDescription = `Setup runtime environment of an installed pod

EXAMPLES

$ p2-setup-runtime podManifest.yaml

$ p2-setup-runtime https://artifactory.local/podManifest.yaml

$ p2-setup-runtime --pod-root '/var/pods' https://artifactory.local/podManifest.yaml
`
)

var (
	app = kingpin.New("p2-setup-runtime", fullDescription)

	manifestURI = app.Arg("manifest", "a path to a pod manifest").Required().URL()
	requireFile = app.Flag(
		"require-file",
		"If set, the p2-exec invocation(s) written for the pod will not execute until the file exists on the system",
	).Default("/dev/shm/p2-may-run").String()
	podRoot       = app.Flag("pod-root", "Pod root directory").Default(pods.DefaultPath).String()
	hooksDir      = app.Flag("hooks-dir", "Directory hooks run scripts are located").Default(hooks.DefaultPath).String()
	dockerUrl     = app.Flag("docker-url", "Docker host url").Default(dockerclient.DefaultDockerHost).String()
	dockerVersion = app.Flag("docker-version", "Docker Version").Default("1.21").String()
	tlsCA         = app.Flag("tls-ca-file", "File containing the x509 PEM-encoded CA ").ExistingFile()
	tlsCert       = app.Flag("tls-cert", "File containing tls cert").ExistingFile()
	tlsKey        = app.Flag("tls-key", "File containing tls key").ExistingFile()
)

func dockerClient() (*dockerclient.Client, error) {
	var client *dockerclient.Client
	var err error

	if *tlsCA != "" && *tlsCert != "" && *tlsKey != "" {
		client, err = dockerclient.NewClientWithOpts(
			dockerclient.WithTLSClientConfig(*tlsCA, *tlsCert, *tlsKey),
			dockerclient.WithHost(*dockerUrl),
			dockerclient.WithVersion(*dockerVersion),
		)
		if err != nil {
			return nil, err
		}
	} else {
		client, err = dockerclient.NewClientWithOpts(
			dockerclient.WithHost(*dockerUrl),
			dockerclient.WithVersion(*dockerVersion),
		)
		if err != nil {
			return nil, err
		}
	}
	return client, err
}

func initPod(manifest manifest.Manifest) (*pods.Pod, error) {
	nodeName, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	fetcher := uri.DefaultFetcher
	podReadOnlyPolicy := pods.NewReadOnlyPolicy(false, nil, nil)
	podFactory := pods.NewFactory(*podRoot, types.NodeName(nodeName), fetcher, *requireFile, podReadOnlyPolicy)

	dockerClient, err := dockerClient()
	if err != nil {
		return nil, err
	}

	podFactory.SetDockerClient(*dockerClient)
	pod := podFactory.NewLegacyPod(manifest.ID())
	return pod, nil
}

func main() {
	app.Version(version.VERSION)
	kingpin.MustParse(app.Parse(os.Args[1:]))

	pods.Log.Logger.Formatter = &logrus.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
		TimestampFormat:  "23:59:59.999",
	}
	logger := pods.Log

	// read manifest
	manifest, err := manifest.FromURI(*manifestURI)
	if err != nil {
		logger.WithError(err).WithField("manifest", *manifestURI).Fatalln("Unable to parse manifest file")
	}

	// list of hook names for which failure is fatal
	hooksRequired := []string{}

	// setup hooks context
	hookLogger := logging.DefaultLogger
	hookLogger.Logger.SetLevel(logrus.ErrorLevel)
	auditLogger := hooks.NewFileAuditLogger(&hookLogger)
	hookContext := hooks.NewContext(*hooksDir, *podRoot, &hookLogger, auditLogger)

	logger = logger.WithField("app", manifest.ID())

	// initialize pod
	pod, err := initPod(manifest)
	if err != nil {
		logger.WithError(err).Fatalln("Failure initializing pod environment")

	}

	launchables, err := pod.Launchables(manifest)
	if err != nil {
		logger.WithError(err).Fatalln("Failure getting launchables from manifest")
	}

	if len(launchables) == 0 {
		logger.Fatalln("There are no launchables in manifest")
	}

	//run hooks BeforeInstall
	logger.Infoln("Running before_install hooks")
	err = hookContext.RunHookType(hooks.BeforeInstall, pod, manifest, hooksRequired)
	if err != nil {
		logger.WithError(err).Fatalln("Failure running before_install hook(s)")
	}

	// run hooks AfterInstall
	logger.Infoln("Running after_install hooks")
	err = hookContext.RunHookType(hooks.AfterInstall, pod, manifest, hooksRequired)
	if err != nil {
		logger.WithError(err).Fatalln("Failure running after_install hook(s)")
	}

	// run hooks BeforeLaunch
	logger.Infoln("Running before_launch hooks")
	err = hookContext.RunHookType(hooks.BeforeLaunch, pod, manifest, hooksRequired)
	if err != nil {
		logger.WithError(err).WithField("app", manifest.ID()).Fatalln("Failure running before_launch hook(s)")
	}

	// run all parts of pods.Launch except for pod.StartLaunchables

	// write current manifest
	logger.Infoln("Writing current manifest")
	oldManifestTemp, err := pod.WriteCurrentManifest(manifest)
	defer os.RemoveAll(oldManifestTemp)
	if err != nil {
		logger.WithError(err).WithField("app", manifest.ID()).Fatalln("Unable to write current manifest")
	}

	// run post activate
	logger.Infoln("Running post-activate")
	err = pod.PostActivate(launchables)
	if err != nil {
		logger.WithError(err).Fatalln("Failure running PostActivate for launchables")
	}

	// build startup services
	logger.Infoln("Running build runit services")
	err = pod.BuildRunitServices(launchables, manifest)
	if err != nil {
		logger.WithError(err).Fatalln("Failure building run scripts")
	}

	logger.Infoln("Success setting up runtime environment for pod: ", manifest.ID())
}
