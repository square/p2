package preparer

import (
	"crypto/tls"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"golang.org/x/net/http2"
	"gopkg.in/yaml.v2"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/statusstore"
	"github.com/square/p2/pkg/kp/statusstore/podstatus"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"
	netutil "github.com/square/p2/pkg/util/net"
	"github.com/square/p2/pkg/util/param"
	"github.com/square/p2/pkg/util/size"
)

// DefaultConsulAddress is the default location for Consul when none is configured.
// TODO: IPv6
const (
	DefaultConsulAddress = "127.0.0.1:8500"

	// Don't change this, it affects where pod status keys are read and written from
	PreparerPodStatusNamespace statusstore.Namespace = "preparer"
)

type AppConfig struct {
	P2PreparerConfig PreparerConfig `yaml:"preparer"`
}

type LogDestination struct {
	Type logging.OutType `yaml:"type"`
	Path string          `yaml:"path"`
}

type Preparer struct {
	node                   types.NodeName
	store                  Store
	podStatusStore         podstatus.Store
	hooks                  Hooks
	hookListener           HookListener
	Logger                 logging.Logger
	podFactory             pods.Factory
	authPolicy             auth.Policy
	maxLaunchableDiskUsage size.ByteCount
	finishExec             []string
	logExec                []string
	logBridgeBlacklist     []string
	artifactVerifier       auth.ArtifactVerifier
	artifactRegistry       artifact.Registry
}

type PreparerConfig struct {
	NodeName               types.NodeName         `yaml:"node_name"`
	ConsulAddress          string                 `yaml:"consul_address"`
	ConsulHttps            bool                   `yaml:"consul_https,omitempty"`
	ConsulTokenPath        string                 `yaml:"consul_token_path,omitempty"`
	HTTP2                  bool                   `yaml:"http2,omitempty"`
	HooksDirectory         string                 `yaml:"hooks_directory"`
	CAFile                 string                 `yaml:"ca_file,omitempty"`
	CertFile               string                 `yaml:"cert_file,omitempty"`
	KeyFile                string                 `yaml:"key_file,omitempty"`
	PodRoot                string                 `yaml:"pod_root,omitempty"`
	StatusPort             int                    `yaml:"status_port"`
	StatusSocket           string                 `yaml:"status_socket"`
	Auth                   map[string]interface{} `yaml:"auth,omitempty"`
	ArtifactAuth           map[string]interface{} `yaml:"artifact_auth,omitempty"`
	ExtraLogDestinations   []LogDestination       `yaml:"extra_log_destinations,omitempty"`
	LogLevel               string                 `yaml:"log_level,omitempty"`
	MaxLaunchableDiskUsage string                 `yaml:"max_launchable_disk_usage"`
	FinishExec             []string               `yaml:"finish_exec,omitempty"`
	LogExec                []string               `yaml:"log_exec,omitempty"`
	LogBridgeBlacklist     []string               `yaml:"log_bridge_blacklist,omitempty"`
	ArtifactRegistryURL    string                 `yaml:"artifact_registry_url,omitempty"`

	// Params defines a collection of miscellaneous runtime parameters defined throughout the
	// source files.
	Params param.Values `yaml:"params"`

	// Use a single consul client so that all requests go through the same HTTP client.
	mux          sync.Mutex
	consulClient consulutil.ConsulClient
}

// --- Deployer ACL strategies ---

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

// --- Artifact verification strategies ---
//
// The type matches one of the auth.Verify* constants
//
// "type: none"     - no artifact verification is done
// "type: build"    - checks that builds have a corresponding signature
// "type: manifest" - checks that builds have corresponding digest manifest and
//  						      manifest signature files.
// "type: either"   - checks that one of "build" or "manifest" strategies pass.
//
type ManifestVerification struct {
	Type           string
	KeyringPath    string   `yaml:"keyring,omitempty"`
	AllowedSigners []string `yaml:"allowed_signers"`
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
	preparerConfig := &appConfig.P2PreparerConfig
	if err != nil {
		return nil, util.Errorf("The config file %s was malformatted - %s", config, err)
	}

	if preparerConfig.NodeName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, util.Errorf("Couldn't determine hostname: %s", err)
		}

		preparerConfig.NodeName = types.NodeName(hostname)
	}
	if preparerConfig.ConsulAddress == "" {
		preparerConfig.ConsulAddress = DefaultConsulAddress
	}
	if preparerConfig.HooksDirectory == "" {
		preparerConfig.HooksDirectory = hooks.DEFAULT_PATH
	}
	if preparerConfig.PodRoot == "" {
		preparerConfig.PodRoot = pods.DefaultPath
	}
	return preparerConfig, nil

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

func (c *PreparerConfig) GetConsulClient() (consulutil.ConsulClient, error) {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.consulClient != nil {
		return c.consulClient, nil
	}
	opts, err := c.getOpts()
	if err != nil {
		return nil, err
	}
	client := kp.NewConsulClient(opts)
	c.consulClient = client
	return client, nil
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

func (c *PreparerConfig) getClient(
	cxnTimeout time.Duration,
	insecureSkipVerify bool,
) (*http.Client, error) {
	tlsConfig, err := netutil.GetTLSConfig(c.CertFile, c.KeyFile, c.CAFile)
	if err != nil {
		return nil, err
	}
	tlsConfig.InsecureSkipVerify = insecureSkipVerify
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
		// same dialer as http.DefaultTransport
		Dial: (&net.Dialer{
			Timeout:   cxnTimeout,
			KeepAlive: cxnTimeout,
		}).Dial,
	}
	if c.HTTP2 {
		if err = http2.ConfigureTransport(transport); err != nil {
			return nil, err
		}
	} else {
		// Disable http2 - as the docs for http.Transport tell us,
		// "If TLSNextProto is nil, HTTP/2 support is enabled automatically."
		// as the Go 1.6 release notes tell us,
		// "Programs that must disable HTTP/2 can do so by setting Transport.TLSNextProto
		// to a non-nil, empty map."
		transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	}
	return &http.Client{Transport: transport}, nil
}

func (c *PreparerConfig) GetClient(cxnTimeout time.Duration) (*http.Client, error) {
	return c.getClient(cxnTimeout, false)
}

func (c *PreparerConfig) GetInsecureClient(cxnTimeout time.Duration) (*http.Client, error) {
	return c.getClient(cxnTimeout, true)
}

func addHooks(preparerConfig *PreparerConfig, logger logging.Logger) {
	for _, dest := range preparerConfig.ExtraLogDestinations {
		logger.WithFields(logrus.Fields{
			"type": dest.Type,
			"path": dest.Path,
		}).Infoln("Adding log destination")
		if err := logger.AddHook(dest.Type, dest.Path); err != nil {
			logger.WithError(err).Errorf("Unable to add log hook. Proceeding.")
		}
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

	authPolicy, err := getDeployerAuth(preparerConfig)
	if err != nil {
		return nil, err
	}

	artifactVerifier, err := getArtifactVerifier(preparerConfig, &logger)
	if err != nil {
		return nil, err
	}

	artifactRegistry, err := getArtifactRegistry(preparerConfig)
	if err != nil {
		return nil, err
	}

	client, err := preparerConfig.GetConsulClient()
	if err != nil {
		return nil, err
	}

	store := kp.NewConsulStore(client)
	statusStore := statusstore.NewConsul(client)
	podStatusStore := podstatus.NewConsul(statusStore, PreparerPodStatusNamespace)

	maxLaunchableDiskUsage := launch.DefaultAllowableDiskUsage
	if preparerConfig.MaxLaunchableDiskUsage != "" {
		maxLaunchableDiskUsage, err = size.Parse(preparerConfig.MaxLaunchableDiskUsage)
		if err != nil {
			return nil, util.Errorf("Unparseable value for max_launchable_disk_usage %v, %v", preparerConfig.MaxLaunchableDiskUsage, err)
		}
	}

	listener := HookListener{
		Intent:           store,
		HookPrefix:       kp.HOOK_TREE,
		Node:             preparerConfig.NodeName,
		HookFactory:      pods.NewHookFactory(filepath.Join(preparerConfig.PodRoot, "hooks"), preparerConfig.NodeName),
		ExecDir:          preparerConfig.HooksDirectory,
		Logger:           logger,
		authPolicy:       authPolicy,
		artifactVerifier: artifactVerifier,
		artifactRegistry: artifactRegistry,
	}

	err = os.MkdirAll(preparerConfig.PodRoot, 0755)
	if err != nil {
		return nil, util.Errorf("Could not create preparer pod directory: %s", err)
	}

	var logExec []string
	if len(preparerConfig.LogExec) > 0 {
		logExec = preparerConfig.LogExec
	} else {
		logExec = runit.DefaultLogExec()
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
		hooks:                  hooks.Hooks(preparerConfig.HooksDirectory, preparerConfig.PodRoot, &logger),
		podStatusStore:         podStatusStore,
		hookListener:           listener,
		Logger:                 logger,
		podFactory:             pods.NewFactory(preparerConfig.PodRoot, preparerConfig.NodeName),
		authPolicy:             authPolicy,
		maxLaunchableDiskUsage: maxLaunchableDiskUsage,
		finishExec:             finishExec,
		logExec:                logExec,
		logBridgeBlacklist:     preparerConfig.LogBridgeBlacklist,
		artifactVerifier:       artifactVerifier,
		artifactRegistry:       artifactRegistry,
	}, nil
}

func getDeployerAuth(preparerConfig *PreparerConfig) (auth.Policy, error) {
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
	return authPolicy, nil
}

func getArtifactVerifier(preparerConfig *PreparerConfig, logger *logging.Logger) (auth.ArtifactVerifier, error) {
	var verif ManifestVerification
	var err error
	switch t, _ := preparerConfig.ArtifactAuth["type"].(string); t {
	case "", auth.VerifyNone:
		return auth.NopVerifier(), nil
	case auth.VerifyManifest:
		err = castYaml(preparerConfig.ArtifactAuth, &verif)
		if err != nil {
			return nil, util.Errorf("error configuring artifact verification: %v", err)
		}
		return auth.NewBuildManifestVerifier(verif.KeyringPath, uri.DefaultFetcher, logger)
	case auth.VerifyBuild:
		err = castYaml(preparerConfig.ArtifactAuth, &verif)
		if err != nil {
			return nil, util.Errorf("error configuring artifact verification: %v", err)
		}
		return auth.NewBuildVerifier(verif.KeyringPath, uri.DefaultFetcher, logger)
	case auth.VerifyEither:
		err = castYaml(preparerConfig.ArtifactAuth, &verif)
		if err != nil {
			return nil, util.Errorf("error configuring artifact verification: %v", err)
		}
		return auth.NewCompositeVerifier(verif.KeyringPath, uri.DefaultFetcher, logger)
	default:
		return nil, util.Errorf("Unrecognized artifact verification type: %v", t)
	}
}

func getArtifactRegistry(preparerConfig *PreparerConfig) (artifact.Registry, error) {
	if preparerConfig.ArtifactRegistryURL == "" {
		// This will still work as long as all launchables have "location" urls specified.
		return artifact.NewRegistry(nil, uri.DefaultFetcher, osversion.DefaultDetector), nil
	}

	url, err := url.Parse(preparerConfig.ArtifactRegistryURL)
	if err != nil {
		return nil, util.Errorf("Could not parse 'artifact_registry_url': %s", err)
	}

	return artifact.NewRegistry(url, uri.DefaultFetcher, osversion.DefaultDetector), nil
}
