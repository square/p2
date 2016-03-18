package preparer

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/yaml.v2"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/util/param"
	"github.com/square/p2/pkg/util/size"
)

// DefaultConsulAddress is the default location for Consul when none is configured.
// TODO: IPv6
const DefaultConsulAddress = "127.0.0.1:8500"

type AppConfig struct {
	P2PreparerConfig PreparerConfig `yaml:"preparer"`
}

type LogDestination struct {
	Type logging.OutType `yaml:"type"`
	Path string          `yaml:"path"`
}

type Preparer struct {
	node                   string
	store                  Store
	hooks                  Hooks
	hookListener           HookListener
	Logger                 logging.Logger
	podRoot                string
	caFile                 string
	authPolicy             auth.Policy
	maxLaunchableDiskUsage size.ByteCount
	finishExec             []string
	logExec                []string
}

type PreparerConfig struct {
	NodeName               string                 `yaml:"node_name"`
	ConsulAddress          string                 `yaml:"consul_address"`
	ConsulHttps            bool                   `yaml:"consul_https,omitempty"`
	ConsulTokenPath        string                 `yaml:"consul_token_path,omitempty"`
	HooksDirectory         string                 `yaml:"hooks_directory"`
	CAFile                 string                 `yaml:"ca_file,omitempty"`
	CertFile               string                 `yaml:"cert_file,omitempty"`
	KeyFile                string                 `yaml:"key_file,omitempty"`
	ConsulCAFile           string                 `yaml:"consul_ca_file,omitempty"`
	PodRoot                string                 `yaml:"pod_root,omitempty"`
	StatusPort             int                    `yaml:"status_port"`
	StatusSocket           string                 `yaml:"status_socket"`
	Auth                   map[string]interface{} `yaml:"auth,omitempty"`
	ExtraLogDestinations   []LogDestination       `yaml:"extra_log_destinations,omitempty"`
	LogLevel               string                 `yaml:"log_level,omitempty"`
	MaxLaunchableDiskUsage string                 `yaml:"max_launchable_disk_usage"`
	FinishExec             []string               `yaml:"finish_exec,omitempty"`
	LogExec                []string               `yaml:"log_exec,omitempty"` // If specifying a path to an executable, we assume it has been specified relative to this preparer's POD_HOME

	// Params defines a collection of miscellaneous runtime parameters defined throughout the
	// source files.
	Params param.Values `yaml:"params"`
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

// LoadConfig reads the preparer's configuration from a file.
func LoadConfig(configPath string) (*PreparerConfig, error) {
	configBytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, util.Errorf("reading config file: %s", err)
	}
	return UnmarshalConfig(configBytes)
}

// UnmarshalConfig reads the preparer's configuration from its bytes.
func UnmarshalConfig(config []byte) (*PreparerConfig, error) {
	appConfig := AppConfig{}
	err := yaml.Unmarshal(config, &appConfig)
	preparerConfig := appConfig.P2PreparerConfig
	if err != nil {
		return nil, util.Errorf("The config file %s was malformatted - %s", config, err)
	}

	if preparerConfig.NodeName == "" {
		preparerConfig.NodeName, _ = os.Hostname()
	}
	if preparerConfig.ConsulAddress == "" {
		preparerConfig.ConsulAddress = DefaultConsulAddress
	}
	if preparerConfig.HooksDirectory == "" {
		preparerConfig.HooksDirectory = hooks.DEFAULT_PATH
	}
	if preparerConfig.PodRoot == "" {
		preparerConfig.PodRoot = pods.DEFAULT_PATH
	}
	return &preparerConfig, nil

}

// loadToken reads the file at the given path and trims its contents for use as a Consul
// token.
func loadToken(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	token, err := ioutil.ReadFile(path)
	if err != nil {
		return "", util.Errorf("reading Consul token: %s", err)
	}
	return strings.TrimSpace(string(token)), nil
}

// getTLSConfig constructs a tls.Config that uses keys/certificates in the given files.
func getTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	var certs []tls.Certificate
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, util.Errorf("TLS client requires both cert file and key file")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, util.Errorf("Could not load keypair: %s", err)
		}
		certs = append(certs, cert)
	}

	var cas *x509.CertPool
	if caFile != "" {
		cas = x509.NewCertPool()
		caBytes, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		ok := cas.AppendCertsFromPEM(caBytes)
		if !ok {
			return nil, util.Errorf("Could not parse certificate file: %s", caFile)
		}
	}

	if len(certs) == 0 && cas == nil {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		Certificates: certs,
		ClientCAs:    cas,
		RootCAs:      cas,
	}
	return tlsConfig, nil
}

// GetStore constructs a key-value store from the given configuration.
func (c *PreparerConfig) GetStore() (kp.Store, error) {
	opts, err := c.getOpts()
	if err != nil {
		return nil, err
	}
	client := kp.NewConsulClient(opts)
	return kp.NewConsulStore(client), nil
}

func (c *PreparerConfig) getOpts() (kp.Options, error) {
	client := http.DefaultClient
	token, err := loadToken(c.ConsulTokenPath)
	if err != nil {
		return kp.Options{}, err
	}

	if c.ConsulHttps {
		client, err = c.GetClient(30 * time.Second) // 30 seconds is the net/http default
		if err != nil {
			return kp.Options{}, err
		}
	}

	return kp.Options{
		Address: c.ConsulAddress,
		HTTPS:   c.ConsulHttps,
		Token:   token,
		Client:  client,
	}, err
}

func (c *PreparerConfig) GetClient(cxnTimeout time.Duration) (*http.Client, error) {
	tlsConfig, err := getTLSConfig(c.CertFile, c.KeyFile, c.CAFile)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: &http.Transport{
		TLSClientConfig: tlsConfig,
		// same dialer as http.DefaultTransport
		Dial: (&net.Dialer{
			Timeout:   cxnTimeout,
			KeepAlive: cxnTimeout,
		}).Dial,
	}}, nil
}

func (c *PreparerConfig) GetInsecureClient(cxnTimeout time.Duration) (*http.Client, error) {
	tlsConfig, err := getTLSConfig(c.CertFile, c.KeyFile, c.CAFile)
	if err != nil {
		return nil, err
	}
	tlsConfig.InsecureSkipVerify = true
	return &http.Client{Transport: &http.Transport{
		TLSClientConfig: tlsConfig,
		// same dialer as http.DefaultTransport
		Dial: (&net.Dialer{
			Timeout:   cxnTimeout,
			KeepAlive: cxnTimeout,
		}).Dial,
	}}, nil
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

	if preparerConfig.LogLevel != "" {
		lv, err := logrus.ParseLevel(preparerConfig.LogLevel)
		if err != nil {
			return nil, util.Errorf("Received invalid log level %q", preparerConfig.LogLevel)
		}
		logger.Logger.Level = lv
	}

	var err error
	var authPolicy auth.Policy
	switch t, _ := preparerConfig.Auth["type"].(string); t {
	case "":
		return nil, util.Errorf("must specify authorization policy type")
	case auth.Null:
		authPolicy = auth.NullPolicy{}
	case auth.Keyring:
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
			map[types.PodID][]string{POD_ID: authConfig.AuthorizedDeployers},
		)
		if err != nil {
			return nil, util.Errorf("error configuring keyring auth: %s", err)
		}
	case auth.User:
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
			POD_ID,
			string(POD_ID),
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

	store, err := preparerConfig.GetStore()
	if err != nil {
		return nil, err
	}

	maxLaunchableDiskUsage := launch.DefaultAllowableDiskUsage
	if preparerConfig.MaxLaunchableDiskUsage != "" {
		maxLaunchableDiskUsage, err = size.Parse(preparerConfig.MaxLaunchableDiskUsage)
		if err != nil {
			return nil, util.Errorf("Unparseable value for max_launchable_disk_usage %v, %v", preparerConfig.MaxLaunchableDiskUsage, err)
		}
	}

	listener := HookListener{
		Intent:         store,
		HookPrefix:     kp.HOOK_TREE,
		Node:           preparerConfig.NodeName,
		DestinationDir: path.Join(pods.DEFAULT_PATH, "hooks"),
		ExecDir:        preparerConfig.HooksDirectory,
		Logger:         logger,
		authPolicy:     authPolicy,
	}

	err = os.MkdirAll(preparerConfig.PodRoot, 0755)
	if err != nil {
		return nil, util.Errorf("Could not create preparer pod directory: %s", err)
	}

	consulCAFile := preparerConfig.ConsulCAFile
	if consulCAFile == "" {
		consulCAFile = preparerConfig.CAFile
	}

	var logExec []string
	if len(preparerConfig.LogExec) > 0 {
		logExec = preparerConfig.LogExec
	} else {
		logExec = pods.DefaultLogExec
	}

	var finishExec []string
	if len(preparerConfig.FinishExec) > 0 {
		finishExec = preparerConfig.FinishExec
	} else {
		finishExec = pods.DefaultFinishExec
	}

	return &Preparer{
		node:                   preparerConfig.NodeName,
		store:                  store,
		hooks:                  hooks.Hooks(preparerConfig.HooksDirectory, &logger),
		hookListener:           listener,
		Logger:                 logger,
		podRoot:                preparerConfig.PodRoot,
		authPolicy:             authPolicy,
		caFile:                 consulCAFile,
		maxLaunchableDiskUsage: maxLaunchableDiskUsage,
		finishExec:             finishExec,
		logExec:                logExec,
	}, nil
}
