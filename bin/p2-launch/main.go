package main

import (
	"log"

	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/preparer"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/version"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v2"
)

var (
	manifestURI  = kingpin.Arg("manifest", "a path or url to a pod manifest that will be installed and launched immediately.").Required().ExistingFile()
	podRoot      = kingpin.Flag("pod-root", "the root of the pods directory").Default(pods.DEFAULT_PATH).Short('p').String()
	authType     = kingpin.Flag("auth-type", "the auth policy to use e.g. (none, keyring, user)").Short('a').Default("none").String()
	keyring      = kingpin.Flag("keyring", "the pgp keyring to use for auth policies if --auth-type other than none is given").Short('k').ExistingFile()
	deployPolicy = kingpin.Flag(
		"deploy-policy",
		"the deploy policy specifying who may deploy each pod. Only used when --auth-type is 'user'",
	).Short('d').ExistingFile()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	manifest, err := pods.ManifestFromURI(*manifestURI)
	if err != nil {
		log.Fatalf("%s", err)
	}

	err = authorize(manifest)
	if err != nil {
		log.Fatalf("%s", err)
	}

	pod := pods.NewPod(manifest.ID(), pods.PodPath(*podRoot, manifest.ID()))
	err = pod.Install(manifest)
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

func authorize(manifest pods.Manifest) error {
	var policy auth.Policy
	var err error
	switch *authType {
	case auth.NULL_POLICY:
		return nil
	case auth.KEYRING_POLICY:
		return util.Errorf("--auth-type of '%s' currently not supported by p2-launch", *authType)
	case auth.USER_POLICY:
		if *keyring == "" {
			return util.Errorf("Must specify --keyring if --auth-type is '%s'", *authType)
		}
		policy, err = auth.NewUserPolicy(
			*keyring,
			*deployPolicy,
			preparer.POD_ID,
			preparer.POD_ID,
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
