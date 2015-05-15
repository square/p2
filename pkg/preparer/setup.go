package preparer

import (
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/util"
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
	NodeName             string                 `yaml:"node_name"`
	ConsulAddress        string                 `yaml:"consul_address"`
	ConsulTokenPath      string                 `yaml:"consul_token_path,omitempty"`
	HooksDirectory       string                 `yaml:"hooks_directory"`
	CAPath               string                 `yaml:"ca_path,omitempty"`
	PodRoot              string                 `yaml:"pod_root,omitempty"`
	StatusPort           int                    `yaml:"status_port"`
	Auth                 map[string]interface{} `yaml:"auth,omitempty"`
	ForbiddenPodIds      []string               `yaml:"forbidden_pod_ids,omitempty"`
	ExtraLogDestinations []LogDestination       `yaml:"extra_log_destinations,omitempty"`
}

// Configuration fields for the "keyring" auth type
type KeyringAuth struct {
	Type                string
	KeyringPath         string   `yaml:"keyring,omitempty"`
	AuthorizedDeployers []string `yaml:"authorized_deployers,omitempty"`
}

// Configuration fields for the "user" auth type
type UserAuth struct {
	Type             string
	KeyringPath      string `yaml:"keyring"`
	DeployPolicyPath string `yaml:"deploy_policy"`
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
	if preparerConfig.PodRoot == "" {
		preparerConfig.PodRoot = pods.DEFAULT_PATH
	}
	return &preparerConfig, nil
}

func loadConsulToken(path string) (string, error) {
	consulToken, err := ioutil.ReadFile(path)
	if err != nil {
		return "", util.Errorf("Could not read Consul token at path %s: %s", path, err)
	}
	return strings.TrimSpace(string(consulToken)), nil
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

// castYaml() allows a YAML block to be reparsed into a struct type by
// re-encoding it into YAML and re-parsing it.
func castYaml(in map[string]interface{}, out interface{}) error {
	encoded, err := yaml.Marshal(in)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(encoded, out)
}

func New(preparerConfig *PreparerConfig, logger logging.Logger) (*Preparer, error) {
	addHooks(preparerConfig, logger)

	if preparerConfig.ConsulAddress == "" {
		return nil, util.Errorf("No Consul address given to the preparer")
	}
	if preparerConfig.PodRoot == "" {
		return nil, util.Errorf("No pod root given to the preparer")
	}

	var err error
	var authPolicy auth.Policy
	switch t, _ := preparerConfig.Auth["type"].(string); t {
	case "":
		return nil, util.Errorf("must specify authorization policy type")
	case "none":
		authPolicy = auth.NullPolicy{}
	case "keyring":
		var authConfig KeyringAuth
		err := castYaml(preparerConfig.Auth, &authConfig)
		if err != nil {
			return nil, util.Errorf("error configuring keyring auth: %s", err)
		}
		if authConfig.KeyringPath == "" {
			return nil, util.Errorf("keyring auth must contain a path to the keyring")
		}
		authPolicy, err = auth.NewFileKeyringPolicy(
			authConfig.KeyringPath,
			map[string][]string{POD_ID: authConfig.AuthorizedDeployers},
		)
		if err != nil {
			return nil, util.Errorf("error configuring keyring auth: %s", err)
		}
	case "user":
		var userConfig UserAuth
		err := castYaml(preparerConfig.Auth, &userConfig)
		if err != nil {
			return nil, util.Errorf("error configuring user auth: %s", err)
		}
		if userConfig.KeyringPath == "" {
			return nil, util.Errorf("user auth must contain a path to the keyring")
		}
		if userConfig.DeployPolicyPath == "" {
			return nil, util.Errorf("user auth must contain a path to the deploy policy")
		}
		authPolicy, err = auth.NewUserPolicy(
			userConfig.KeyringPath,
			userConfig.DeployPolicyPath,
		)
		if err != nil {
			return nil, util.Errorf("error configuring user auth: %s", err)
		}
	default:
		if t, ok := preparerConfig.Auth["type"].(string); ok {
			return nil, util.Errorf("unrecognized auth type: %s", t)
		}
		return nil, util.Errorf("unrecognized auth type")
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
		authPolicy:     authPolicy,
	}

	err = os.MkdirAll(preparerConfig.PodRoot, 0755)
	if err != nil {
		return nil, util.Errorf("Could not create preparer pod directory: %s", err)
	}

	forbiddenPodIds := make(map[string]struct{})
	for _, forbidden := range preparerConfig.ForbiddenPodIds {
		forbiddenPodIds[forbidden] = struct{}{}
	}

	return &Preparer{
		node:            preparerConfig.NodeName,
		store:           store,
		hooks:           hooks.Hooks(preparerConfig.HooksDirectory, &logger),
		hookListener:    listener,
		Logger:          logger,
		podRoot:         preparerConfig.PodRoot,
		forbiddenPodIds: forbiddenPodIds,
		authPolicy:      authPolicy,
		caPath:          preparerConfig.CAPath,
	}, nil
}
