package main

import (
	"log"
	"net"
	"net/http"
	"os"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/constants"
	"github.com/square/p2/pkg/docker"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
	netutil "github.com/square/p2/pkg/util/net"
	"github.com/square/p2/pkg/version"

	"github.com/Sirupsen/logrus"
	dockerclient "github.com/moby/moby/client"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	manifestURI  = kingpin.Arg("manifest", "a path to a pod manifest that will be installed and launched immediately.").Required().URL()
	nodeName     = kingpin.Flag("node-name", "the name of this node (default: hostname)").String()
	podRoot      = kingpin.Flag("pod-root", "the root of the pods directory").Default(pods.DefaultPath).Short('p').String()
	authType     = kingpin.Flag("auth-type", "the auth policy to use e.g. (none, keyring, user)").Short('a').Default("none").String()
	keyring      = kingpin.Flag("keyring", "the pgp keyring to use for auth policies if --auth-type other than none is given").Short('k').ExistingFile()
	allowedUsers = kingpin.Flag("allowed-user", "a user allowed to deploy. may be specified more than once. only necessary when '--auth-type keyring' is used").Short('u').Strings()
	deployPolicy = kingpin.Flag(
		"deploy-policy",
		"the deploy policy specifying who may deploy each pod. Only used when --auth-type is 'user'",
	).Short('d').ExistingFile()
	caFile                        = kingpin.Flag("tls-ca-file", "File containing the x509 PEM-encoded CA ").ExistingFile()
	artifactRegistryURL           = kingpin.Flag("artifact-registry-url", "the artifact registry to fetch artifacts from").Short('r').URL()
	requireFile                   = kingpin.Flag("require-file", "If set, the p2-exec invocation(s) written for the pod will not execute until the file exists on the system").String()
	containerRegistryJsonKeyFile  = kingpin.Flag("container-registry-json-key", "Need to set when trying to launch a pod with docker launchables").ExistingFile()
	dockerImageDirectoryWhitelist = kingpin.Flag("docker-image-directory-whitelist", "Need to set when trying to launch a pod with docker launchables").Strings()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	if *nodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalf("error getting node name: %v", err)
		}
		*nodeName = hostname
	}

	manifest, err := manifest.FromURI(*manifestURI)
	if err != nil {
		log.Fatalf("%s", err)
	}

	err = authorize(manifest)
	if err != nil {
		log.Fatalf("%s", err)
	}

	var transport http.RoundTripper
	if *caFile != "" {
		tlsConfig, err := netutil.GetTLSConfig("", "", *caFile)
		if err != nil {
			log.Fatalln(err)
		}

		transport = &http.Transport{
			TLSClientConfig: tlsConfig,
			// same dialer as http.DefaultTransport
			Dial: (&net.Dialer{
				Timeout:   http.DefaultClient.Timeout,
				KeepAlive: http.DefaultClient.Timeout,
			}).Dial,
		}
	} else {
		transport = http.DefaultTransport
	}

	httpClient := &http.Client{
		Transport: transport,
	}

	fetcher := uri.BasicFetcher{Client: httpClient}

	podFactory := pods.NewFactory(*podRoot, types.NodeName(*nodeName), fetcher, *requireFile, pods.NewReadOnlyPolicy(false, nil, nil))
	dockerClient, err := dockerclient.NewEnvClient()
	if err != nil {
		log.Fatalf("could not create docker client: %s", err)
	}
	podFactory.SetDockerClient(*dockerClient)
	pod := podFactory.NewLegacyPod(manifest.ID())

	containerRegistryAuthStr := ""
	if *containerRegistryJsonKeyFile != "" {
		containerRegistryAuthStr, err = docker.GetContainerRegistryAuthStr(*containerRegistryJsonKeyFile)
		if err != nil {
			log.Fatalf("error getting container registry auth string: %s", err)
		}
	}
	err = pod.Install(manifest, auth.NopVerifier(), artifact.NewRegistry(*artifactRegistryURL, fetcher, osversion.DefaultDetector), containerRegistryAuthStr, *dockerImageDirectoryWhitelist)
	if err != nil {
		log.Fatalf("Could not install manifest %s: %s", manifest.ID(), err)
	}

	success, err := pod.Launch(manifest)
	if err != nil {
		log.Fatalf("Could not launch manifest %s: %s", manifest.ID(), err)
	}
	if !success {
		log.Fatalln("Unsuccessful launch of one or more things in the manifest")
	}
}

func authorize(manifest manifest.Manifest) error {
	var policy auth.Policy
	var err error
	switch *authType {
	case auth.Null:
		if *keyring != "" {
			return util.Errorf("--keyring may not be specified if --auth-type is '%s'", *authType)
		}
		if *deployPolicy != "" {
			return util.Errorf("--deploy-policy may not be specified if --auth-type is '%s'", *authType)
		}
		if len(*allowedUsers) != 0 {
			return util.Errorf("--allowed-users may not be specified if --auth-type is '%s'", *authType)
		}

		return nil
	case auth.Keyring:
		if *keyring == "" {
			return util.Errorf("Must specify --keyring if --auth-type is '%s'", *authType)
		}
		if len(*allowedUsers) == 0 {
			return util.Errorf("Must specify at least one allowed user if using a keyring auth type")
		}

		policy, err = auth.NewFileKeyringPolicy(
			*keyring,
			map[types.PodID][]string{
				constants.PreparerPodID: *allowedUsers,
			},
		)
		if err != nil {
			return err
		}
	case auth.User:
		if *keyring == "" {
			return util.Errorf("Must specify --keyring if --auth-type is '%s'", *authType)
		}
		if *deployPolicy == "" {
			return util.Errorf("Must specify --deploy-policy if --auth-type is '%s'", *authType)
		}

		policy, err = auth.NewUserPolicy(
			*keyring,
			*deployPolicy,
			constants.PreparerPodID,
			constants.PreparerPodID.String(),
		)
		if err != nil {
			return err
		}
	default:
		return util.Errorf("Unknown --auth-type: %s", *authType)
	}

	logger := logging.NewLogger(logrus.Fields{})
	logger.Logger.Formatter = new(logrus.TextFormatter)

	err = policy.AuthorizeApp(manifest, logger)
	if err != nil {
		if err, ok := err.(auth.Error); ok {
			logger.WithFields(err.Fields).Errorln(err)
		} else {
			logger.NoFields().Errorln(err)
		}
		return err
	}

	return nil
}
