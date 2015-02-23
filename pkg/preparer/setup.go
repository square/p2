package preparer

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
	"golang.org/x/crypto/openpgp"
	"gopkg.in/yaml.v2"
)

type AppConfig struct {
	P2PreparerConfig PreparerConfig `yaml:"preparer"`
}

type LogDestination struct {
	Type logging.OutType `yaml:"type"`
	Path string          `yaml:"path"`
}

type PreparerConfig struct {
	NodeName             string           `yaml:"node_name"`
	ConsulAddress        string           `yaml:"consul_address"`
	ConsulTokenPath      string           `yaml:"consul_token_path,omitempty"`
	HooksDirectory       string           `yaml:"hooks_directory"`
	KeyringPath          string           `yaml:"keyring,omitempty"`
	ExtraLogDestinations []LogDestination `yaml:"extra_log_destinations,omitempty"`
}

func LoadPreparerConfig(configPath string) (*PreparerConfig, error) {
	configBytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, util.Errorf("Could not read the config file %s - %s", configPath, err)
	}
	appConfig := AppConfig{}
	err = yaml.Unmarshal(configBytes, &appConfig)
	preparerConfig := appConfig.P2PreparerConfig
	if err != nil {
		return nil, util.Errorf("The config file %s was malformatted - %s", configPath, err)
	}

	if preparerConfig.NodeName == "" {
		preparerConfig.NodeName, _ = os.Hostname()
	}
	if preparerConfig.ConsulAddress == "" {
		preparerConfig.ConsulAddress = "127.0.0.1:8500"
	}
	if preparerConfig.HooksDirectory == "" {
		preparerConfig.HooksDirectory = hooks.DEFAULT_PATH
	}
	return &preparerConfig, nil
}

func loadKeyring(path string) (openpgp.KeyRing, error) {
	if path == "" {
		return nil, util.Errorf("No keyring configured")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return openpgp.ReadKeyRing(f)
}

func loadConsulToken(path string) (string, error) {
	consulToken, err := ioutil.ReadFile(path)
	if err != nil {
		return "", util.Errorf("Could not read Consul token at path %s: %s", path, err)
	}
	return string(consulToken), nil
}

func addHooks(preparerConfig *PreparerConfig, logger logging.Logger) {
	for _, dest := range preparerConfig.ExtraLogDestinations {
		logger.WithFields(logrus.Fields{
			"type": dest.Type,
			"path": dest.Path,
		}).Infoln("Adding log destination")
		logger.AddHook(dest.Type, dest.Path)
	}
}

func NewPreparer(preparerConfig *PreparerConfig, logger logging.Logger) (*Preparer, error) {
	addHooks(preparerConfig, logger)

	if preparerConfig.ConsulAddress == "" {
		return nil, util.Errorf("No Consul address given to the preparer")
	}
	var err error
	var keyring openpgp.KeyRing = nil
	if preparerConfig.KeyringPath != "" {
		keyring, err = loadKeyring(preparerConfig.KeyringPath)
		if err != nil {
			return nil, util.Errorf("The keyring at path %s was not valid: %s", preparerConfig.KeyringPath, err)
		}
	}

	consulToken := ""
	if preparerConfig.ConsulTokenPath != "" {
		consulToken, err = loadConsulToken(preparerConfig.ConsulTokenPath)
		if err != nil {
			return nil, err
		}
	}

	store := kp.NewStore(kp.Options{
		Address: preparerConfig.ConsulAddress,
		Token:   consulToken,
	})

	listener := HookListener{
		Intent:         store,
		HookPrefix:     kp.HOOK_TREE,
		DestinationDir: path.Join(pods.DEFAULT_PATH, "hooks"),
		ExecDir:        preparerConfig.HooksDirectory,
		Logger:         logger,
		Keyring:        keyring,
	}

	err = os.MkdirAll(pods.DEFAULT_PATH, 0755)
	if err != nil {
		logger.WithField("inner_err", err).Errorln("Could not create preparer pod directory")
		os.Exit(1)
	}

	return &Preparer{
		node:         preparerConfig.NodeName,
		store:        store,
		hooks:        hooks.Hooks(preparerConfig.HooksDirectory, &logger),
		hookListener: listener,
		Logger:       logger,
		keyring:      keyring,
	}, nil
}
