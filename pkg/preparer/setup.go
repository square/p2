package preparer

import (
	"crypto/tls"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	dockerapi "github.com/docker/docker/api"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/go-connections/tlsconfig"
	"github.com/hashicorp/consul/api"
	context "golang.org/x/net/context"
	"golang.org/x/net/http2"
	"gopkg.in/yaml.v2"

	"github.com/square/p2/pkg/artifact"
	"github.com/square/p2/pkg/auth"
	"github.com/square/p2/pkg/constants"
	"github.com/square/p2/pkg/docker"
	"github.com/square/p2/pkg/hooks"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/osversion"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/preparer/podprocess"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/podstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/podstatus"
	"github.com/square/p2/pkg/store/consul/transaction"
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

	// Can be provided in place of the hook manifest in config to instruct
	// the preparer to start without hooks.
	NoHooksSentinelValue = "no_hooks"
)

type AppConfig struct {
	P2PreparerConfig PreparerConfig `yaml:"preparer"`
}

type LogDestination struct {
	Type logging.OutType `yaml:"type"`
	Path string          `yaml:"path"`
}

type PodStatusStore interface {
	Get(key types.PodUniqueKey) (podstatus.PodStatus, *api.QueryMeta, error)
	MutateStatus(ctx context.Context, key types.PodUniqueKey, mutator func(podstatus.PodStatus) (podstatus.PodStatus, error)) error
}

type Preparer struct {
	node                   types.NodeName
	store                  Store
	podStatusStore         PodStatusStore
	podStore               podstore.Store
	client                 consulutil.ConsulClient
	hooks                  Hooks
	Logger                 logging.Logger
	podFactory             pods.Factory
	podRoot                string
	authPolicy             auth.Policy
	maxLaunchableDiskUsage size.ByteCount
	finishExec             []string
	logExec                []string
	logBridgeBlacklist     []string
	artifactVerifier       auth.ArtifactVerifier
	artifactRegistry       artifact.Registry
	fetcher                uri.Fetcher // cached (potentially nil) uri.Fetcher configured based on the preparer's manifest

	// Exported so it can be checked for nil (it only runs if configured)
	// and quit channel conditially created
	PodProcessReporter *podprocess.Reporter

	// The pod manifest to use for hooks
	hooksManifest manifest.Manifest

	// The pod to use for hooks
	hooksPod *pods.Pod

	// The directory that will actually be executed by the HookDir
	hooksExecDir string

	// base64 encoding of docker authConfig needed for ImagePull
	containerRegistryAuthStr string
}

type store interface {
	SetPod(podPrefix consul.PodPrefix, nodename types.NodeName, manifest manifest.Manifest) (time.Duration, error)
	Pod(podPrefix consul.PodPrefix, nodename types.NodeName, podId types.PodID) (manifest.Manifest, time.Duration, error)
	DeletePod(podPrefix consul.PodPrefix, nodename types.NodeName, podId types.PodID) (time.Duration, error)
	ListPods(podPrefix consul.PodPrefix, nodename types.NodeName) ([]consul.ManifestResult, time.Duration, error)
	WatchPods(
		podPrefix consul.PodPrefix,
		hostname types.NodeName,
		quit <-chan struct{},
		errCh chan<- error,
		manifests chan<- []consul.ManifestResult,
	)
}

// ConsulConfig encapsulates config options related to how p2-preparer
// interacts with consul.
// TODO: move ConsulAddress, ConsulHttps, ConsulTokenPath here.
type ConsulConfig struct {
	// WaitTime specifies the timeout length for HTTP watches on consul. Longer
	// values mean longer lived requests and therefore lower QPS and bandwidth
	// usage when there are infrequent changes to the watched data
	WatchWaitTime time.Duration `yaml:"watch_wait_time"`
}

type PreparerConfig struct {
	NodeName                     types.NodeName         `yaml:"node_name"`
	ConsulAddress                string                 `yaml:"consul_address"`
	ConsulHttps                  bool                   `yaml:"consul_https,omitempty"`
	ConsulTokenPath              string                 `yaml:"consul_token_path,omitempty"`
	HTTP2                        bool                   `yaml:"http2,omitempty"`
	HooksDirectory               string                 `yaml:"hooks_directory"`
	CAFile                       string                 `yaml:"ca_file,omitempty"`
	CertFile                     string                 `yaml:"cert_file,omitempty"`
	KeyFile                      string                 `yaml:"key_file,omitempty"`
	PodRoot                      string                 `yaml:"pod_root,omitempty"`
	RequireFile                  string                 `yaml:"require_file,omitempty"`
	PodWhitelistFile             string                 `yaml:"pod_whitelist_file,omitempty"`
	StatusPort                   int                    `yaml:"status_port"`
	StatusSocket                 string                 `yaml:"status_socket"`
	Auth                         map[string]interface{} `yaml:"auth,omitempty"`
	ArtifactAuth                 map[string]interface{} `yaml:"artifact_auth,omitempty"`
	ExtraLogDestinations         []LogDestination       `yaml:"extra_log_destinations,omitempty"`
	LogLevel                     string                 `yaml:"log_level,omitempty"`
	MaxLaunchableDiskUsage       string                 `yaml:"max_launchable_disk_usage"`
	LogExec                      []string               `yaml:"log_exec,omitempty"`
	LogBridgeBlacklist           []string               `yaml:"log_bridge_blacklist,omitempty"`
	ArtifactRegistryURL          string                 `yaml:"artifact_registry_url,omitempty"`
	DockerHost                   string                 `yaml:"docker_host,omitempty"`
	ContainerRegistryJsonKeyFile string                 `yaml:"container_json_key_file,omitempty"`
	ConsulConfig                 ConsulConfig           `yaml:"consul_config,omitempty"`

	OSVersionFile string `yaml:"os_version_file,omitempty"`

	ReadOnlyDeploys   bool          `yaml:"read_only_deploys"`
	ReadOnlyWhitelist []types.PodID `yaml:"read_only_whitelist"`
	ReadOnlyBlacklist []types.PodID `yaml:"read_only_blacklist"`

	// The pod manifest to use for hooks. If no hooks are desired, use the
	// NoHooksSentinelValue constant to indicate that there aren't any
	HooksManifest string `yaml:"hooks_manifest,omitempty"`

	// Configures reporting the exit status of processes started by a pod to Consul
	PodProcessReporterConfig podprocess.ReporterConfig `yaml:"process_result_reporter_config"`

	// Params defines a collection of miscellaneous runtime parameters defined throughout the
	// source files.
	Params param.Values `yaml:"params"`

	// HTTPTimeout is the timeout that will be set on the preparer's HTTP
	// client. This is pretty coarse grained at the moment, it's possibly
	// desirable to be able to set a different HTTP timeout for different
	// clients, e.g. consul client vs artifact downloader
	HTTPTimeout time.Duration `yaml:"http_timeout"`

	// Use a single Store so that all requests go through the same HTTP client.
	consulClientMux sync.Mutex
	consulClient    consulutil.ConsulClient

	httpClientMux sync.Mutex
	httpClient    *http.Client
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
		preparerConfig.HooksDirectory = hooks.DefaultPath
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
	c.consulClientMux.Lock()
	defer c.consulClientMux.Unlock()
	if c.consulClient != nil {
		return c.consulClient, nil
	}
	opts, err := c.getOpts()
	if err != nil {
		return nil, err
	}
	client := consul.NewConsulClient(opts)
	c.consulClient = client
	return client, nil
}

func (c *PreparerConfig) getOpts() (consul.Options, error) {
	client := http.DefaultClient
	token, err := loadToken(c.ConsulTokenPath)
	if err != nil {
		return consul.Options{}, err
	}

	if c.ConsulHttps {
		client, err = c.GetClient(30 * time.Second) // 30 seconds is the net/http default
		if err != nil {
			return consul.Options{}, err
		}
	}

	// Put a lower bound on wait time of 5 minutes
	waitTime := c.ConsulConfig.WatchWaitTime
	if waitTime < 5*time.Minute {
		waitTime = 5 * time.Minute
	}
	return consul.Options{
		Address:  c.ConsulAddress,
		HTTPS:    c.ConsulHttps,
		Token:    token,
		Client:   client,
		WaitTime: waitTime,
	}, err
}

func (c *PreparerConfig) getClient(
	cxnTimeout time.Duration,
	insecureSkipVerify bool,
) (*http.Client, error) {
	c.httpClientMux.Lock()
	defer c.httpClientMux.Unlock()
	if c.httpClient != nil {
		return c.httpClient, nil
	}

	tlsConfig, err := netutil.GetTLSConfig(c.CertFile, c.KeyFile, c.CAFile)
	if err != nil {
		return nil, err
	}

	if cxnTimeout < 30*time.Second {
		cxnTimeout = 30 * time.Second
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

	c.httpClient = &http.Client{
		Transport: transport,
		// 6 minutes is slightly higher than the wait time we use on consul watches of
		// 5 minutes. We expect that a response might not come back for up to 5
		// minutes, but we shouldn't wait much longer than that
		Timeout: 6 * time.Minute,
	}
	return c.httpClient, nil
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

	statusStore := statusstore.NewConsul(client)
	podStatusStore := podstatus.NewConsul(statusStore, consul.PreparerPodStatusNamespace)
	podStore := podstore.NewConsul(client.KV())

	store := consul.NewConsulStore(client)

	maxLaunchableDiskUsage := launch.DefaultAllowableDiskUsage
	if preparerConfig.MaxLaunchableDiskUsage != "" {
		maxLaunchableDiskUsage, err = size.Parse(preparerConfig.MaxLaunchableDiskUsage)
		if err != nil {
			return nil, util.Errorf("Unparseable value for max_launchable_disk_usage %v, %v", preparerConfig.MaxLaunchableDiskUsage, err)
		}
	}

	err = os.MkdirAll(preparerConfig.PodRoot, 0755)
	if err != nil {
		return nil, util.Errorf("Could not create preparer pod directory: %s", err)
	}

	// Artifact files are downloaded to os.TempDir().
	// Since we extract artifact files as target user, we must allow them to access the tmpdir.
	// We expect that there is no sensitive information in TempDir, so 755 is safe, though 711 could be considered.
	tmpDirStat, err := os.Stat(os.TempDir())
	if err != nil {
		return nil, util.Errorf("Could not stat tmpdir: %s", err)
	}
	mode := tmpDirStat.Mode()
	// We don't chmod if the directory is already 755.
	// Normally there is no harm in doing so,
	// but on Travis we don't have permission to do so (we don't run as root).

	currUser, err := user.Current()
	if err != nil {
		return nil, err
	}
	if mode&0755 != 0755 && currUser.Uid == "0" {
		// keep whatever upper bit is there.
		err = os.Chmod(os.TempDir(), (mode&07000)|0755)
		if err != nil {
			return nil, util.Errorf("Could not chmod tmpdir: %s", err)
		}
	}

	var logExec []string
	if len(preparerConfig.LogExec) > 0 {
		logExec = preparerConfig.LogExec
	} else {
		logExec = runit.DefaultLogExec()
	}

	finishExec := pods.NopFinishExec
	var podProcessReporter *podprocess.Reporter
	if preparerConfig.PodProcessReporterConfig.FullyConfigured() {
		podProcessReporterLogger := logger.SubLogger(logrus.Fields{
			"component": "PodProcessReporter",
		})

		podProcessReporter, err = podprocess.New(preparerConfig.PodProcessReporterConfig, podProcessReporterLogger, podStatusStore, client)
		if err != nil {
			return nil, err
		}

		finishExec = preparerConfig.PodProcessReporterConfig.FinishExec()
	}

	// TODO: probably set up a different HTTP client for artifact downloads and other operations, we might want different timeouts for each.
	httpClient, err := preparerConfig.GetClient(preparerConfig.HTTPTimeout)
	if err != nil {
		return nil, err
	}
	fetcher := uri.BasicFetcher{
		Client: httpClient,
	}

	var hooksManifest manifest.Manifest
	var hooksPod *pods.Pod
	var auditLogger hooks.AuditLogger
	auditLogger = hooks.NewFileAuditLogger(&logger)
	if preparerConfig.HooksManifest != NoHooksSentinelValue {
		if preparerConfig.HooksManifest == "" {
			return nil, util.Errorf("Most provide a hooks_manifest or sentinel value %q to indicate that there are no hooks", NoHooksSentinelValue)
		}

		hooksManifest, err = manifest.FromBytes([]byte(preparerConfig.HooksManifest))
		if err != nil {
			return nil, util.Errorf("Could not parse configured hooks manifest: %s", err)
		}
		hooksPodFactory := pods.NewHookFactory(filepath.Join(preparerConfig.PodRoot, "hooks"), preparerConfig.NodeName, fetcher)
		hooksPod = hooksPodFactory.NewHookPod(hooksManifest.ID())
		hooksSqlite, ok := hooksManifest.GetConfig()["sqlite_path"]
		if ok {
			sqlitePath := hooksSqlite.(string)
			if err = os.MkdirAll(path.Dir(sqlitePath), os.ModeDir); err != nil {
				err = os.Chmod(sqlitePath, 0777)
			}

			if err != nil {
				logger.WithError(err).Errorf("Unable to construct a SQLite based audit-logger. Using file backed instead")
			} else {
				al, err := hooks.NewSQLiteAuditLogger(sqlitePath, &logger)
				if err != nil {
					logger.Errorf("Unable to construct a SQLite based audit-logger. Using file backed instead: %v", err)
				} else {
					auditLogger = al
				}
			}

		}
	}

	readOnlyPolicy := pods.NewReadOnlyPolicy(preparerConfig.ReadOnlyDeploys, preparerConfig.ReadOnlyWhitelist, preparerConfig.ReadOnlyBlacklist)

	osVersionDetector := osversion.DefaultDetector
	if preparerConfig.OSVersionFile != "" {
		osVersionDetector = osversion.NewDetector(preparerConfig.OSVersionFile)
	}

	podFactory := pods.NewFactory(preparerConfig.PodRoot, preparerConfig.NodeName, fetcher, preparerConfig.RequireFile, readOnlyPolicy)
	podFactory.SetOSVersionDetector(osVersionDetector)

	// setup docker client
	options := tlsconfig.Options{
		CAFile:             preparerConfig.CAFile,
		CertFile:           preparerConfig.CertFile,
		KeyFile:            preparerConfig.KeyFile,
		InsecureSkipVerify: false,
	}
	tlsc, err := tlsconfig.Client(options)
	if err != nil {
		return nil, util.Errorf("could not setup tlsconfig for docker client: %s", err)
	}
	dockerHTTPClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsc,
		},
	}
	dockerHost := preparerConfig.DockerHost
	if dockerHost == "" {
		dockerHost = dockerclient.DefaultDockerHost
	}
	version := os.Getenv("DOCKER_API_VERSION")
	if version == "" {
		version = dockerapi.DefaultVersion
	}
	dockerClient, err := dockerclient.NewClient(dockerHost, version, dockerHTTPClient, nil)
	if err != nil {
		return nil, util.Errorf("could not create docker client: %s", err)
	}

	containerRegistryAuthStr := ""
	if preparerConfig.ContainerRegistryJsonKeyFile != "" {
		containerRegistryAuthStr, err = docker.GetContainerRegistryAuthStr(preparerConfig.ContainerRegistryJsonKeyFile)
		if err != nil {
			return nil, util.Errorf("error getting container registry auth string: %s", err)
		}
	}

	podFactory.SetDockerClient(*dockerClient)
	return &Preparer{
		node:                     preparerConfig.NodeName,
		store:                    store,
		hooks:                    hooks.NewContext(preparerConfig.HooksDirectory, preparerConfig.PodRoot, &logger, auditLogger),
		podStatusStore:           podStatusStore,
		podStore:                 podStore,
		podRoot:                  preparerConfig.PodRoot,
		client:                   client,
		Logger:                   logger,
		podFactory:               podFactory,
		authPolicy:               authPolicy,
		maxLaunchableDiskUsage:   maxLaunchableDiskUsage,
		finishExec:               finishExec,
		logExec:                  logExec,
		logBridgeBlacklist:       preparerConfig.LogBridgeBlacklist,
		artifactVerifier:         artifactVerifier,
		artifactRegistry:         artifactRegistry,
		containerRegistryAuthStr: containerRegistryAuthStr,
		PodProcessReporter:       podProcessReporter,
		hooksManifest:            hooksManifest,
		hooksPod:                 hooksPod,
		hooksExecDir:             preparerConfig.HooksDirectory,
		fetcher:                  fetcher,
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
			map[types.PodID][]string{constants.PreparerPodID: authConfig.AuthorizedDeployers},
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
			constants.PreparerPodID,
			constants.PreparerPodID.String(),
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
	httpClient, err := preparerConfig.GetClient(30 * time.Second)
	if err != nil {
		return nil, err
	}

	fetcher := uri.BasicFetcher{
		Client: httpClient,
	}
	var verif ManifestVerification
	switch t, _ := preparerConfig.ArtifactAuth["type"].(string); t {
	case "", auth.VerifyNone:
		return auth.NopVerifier(), nil
	case auth.VerifyManifest:
		err = castYaml(preparerConfig.ArtifactAuth, &verif)
		if err != nil {
			return nil, util.Errorf("error configuring artifact verification: %v", err)
		}
		return auth.NewBuildManifestVerifier(verif.KeyringPath, fetcher, logger)
	case auth.VerifyBuild:
		err = castYaml(preparerConfig.ArtifactAuth, &verif)
		if err != nil {
			return nil, util.Errorf("error configuring artifact verification: %v", err)
		}
		return auth.NewBuildVerifier(verif.KeyringPath, fetcher, logger)
	case auth.VerifyEither:
		err = castYaml(preparerConfig.ArtifactAuth, &verif)
		if err != nil {
			return nil, util.Errorf("error configuring artifact verification: %v", err)
		}
		return auth.NewCompositeVerifier(verif.KeyringPath, fetcher, logger)
	default:
		return nil, util.Errorf("Unrecognized artifact verification type: %v", t)
	}
}

func getArtifactRegistry(preparerConfig *PreparerConfig) (artifact.Registry, error) {
	httpClient, err := preparerConfig.GetClient(30 * time.Second)
	if err != nil {
		return nil, err
	}

	fetcher := uri.BasicFetcher{
		Client: httpClient,
	}

	if preparerConfig.ArtifactRegistryURL == "" {
		// This will still work as long as all launchables have "location" urls specified.
		return artifact.NewRegistry(nil, fetcher, osversion.DefaultDetector), nil
	}

	url, err := url.Parse(preparerConfig.ArtifactRegistryURL)
	if err != nil {
		return nil, util.Errorf("Could not parse 'artifact_registry_url': %s", err)
	}

	return artifact.NewRegistry(url, fetcher, osversion.DefaultDetector), nil
}

func (p *Preparer) BuildRealityAtLaunch() error {
	// check for pods on disk and not in intent
	// insert kv pairs into reality if so
	sub := p.Logger.SubLogger(nil)
	// get set of pods in reality
	realityResults, _, err := p.store.ListPods(consul.REALITY_TREE, p.node)
	if err != nil {
		sub.WithError(err).Errorln("Could not check reality: %s", err)
		return err
	}
	realityMap := map[string]bool{}
	for _, result := range realityResults {
		realityMap[result.Manifest.ID().String()] = true
	}
	// get set of pods in intent
	intentResults, _, err := p.store.ListPods(consul.INTENT_TREE, p.node)
	if err != nil {
		sub.WithError(err).Errorln("Could not check intent: %s", err)
		return err
	}
	intentMap := map[string]bool{}
	for _, result := range intentResults {
		intentMap[result.Manifest.ID().String()] = true
	}
	// get set of pods installed on machine
	manifestFilePaths, err := filepath.Glob(filepath.Join(p.podRoot, "*/current_manifest.yaml"))
	if err != nil {
		sub.WithError(err).Errorln("Could not check filepaths of pods installed on disk: %s", err)
		return err
	}
	for _, fp := range manifestFilePaths {
		home := strings.SplitN(fp, "/", 5)[3]
		podUUID := types.HomeToPodUUID(home)
		if podUUID != nil {
			uniqueKey := types.PodUniqueKey(podUUID.String())
			_, err := p.podStore.ReadPodFromIndex(podstore.PodIndex{PodKey: uniqueKey})
			// no pod means the pod was unscheduled and also removed from the intent tree
			if podstore.IsNoPod(err) {
				ctx, cancelFunc := transaction.New(context.Background())
				defer cancelFunc()
				err := p.podStore.WriteRealityIndex(ctx, uniqueKey, p.node)
				if err != nil {
					sub.WithError(err).Errorln("Could not add 'write uuid index to reality store' to transaction: %s", err)
					return err
				}
				ok, resp, err := transaction.Commit(ctx, p.client.KV())
				if err != nil {
					sub.WithError(err).Errorln("Could not write uuid index to reality store")
					return err
				}
				if !ok {
					err := util.Errorf("status record transaction rolled back: %s", transaction.TxnErrorsToString(resp.Errors))
					sub.WithError(err).Errorln("Could not write uuid index to reality store")
					return err
				}
			} else if err != nil {
				sub.WithError(err).Errorln("Unexpected error reading pod: %s", err)
				return err
			}
		} else if _, ok := intentMap[home]; !ok {
			if _, ok := realityMap[home]; !ok {
				diskManifest, err := manifest.FromPath(fp)
				if err != nil {
					sub.WithError(err).Errorln("Could not read manifest from path: %s", err)
					return err
				}
				_, err = p.store.SetPod(consul.REALITY_TREE, p.node, diskManifest)
				if err != nil {
					sub.WithError(err).Errorln("Could not set pod in reality tree: %s", err)
					return err
				}
			}
		}
	}
	return nil
}

func (p *Preparer) InstallHooks() error {
	if p.hooksManifest == nil {
		p.Logger.Infoln("No hooks configured, skipping hook installation")
		return nil
	}

	sub := p.Logger.SubLogger(logrus.Fields{
		"pod": p.hooksManifest.ID(),
	})

	p.Logger.Infoln("Installing hook manifest")
	registry := p.artifactRegistryFor(p.hooksManifest)
	err := p.hooksPod.Install(p.hooksManifest, p.artifactVerifier, registry, "")
	if err != nil {
		sub.WithError(err).Errorln("Could not install hook")
		return err
	}

	_, err = p.hooksPod.WriteCurrentManifest(p.hooksManifest)
	if err != nil {
		sub.WithError(err).Errorln("Could not write current manifest")
		return err
	}

	// Now that the pod is installed, link it up to the exec dir.
	err = hooks.InstallHookScripts(p.hooksExecDir, p.hooksPod, p.hooksManifest, sub)
	if err != nil {
		sub.WithError(err).Errorln("Could not write hook link")
		return err
	}
	sub.NoFields().Infoln("Updated hook")

	p.hooksPod.Prune(p.maxLaunchableDiskUsage, p.hooksManifest)

	return nil
}
