package pods

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/p2exec"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
)

const DefaultPath = "/data/pods"

var (
	Log logging.Logger
)

func init() {
	Log = logging.NewLogger(logrus.Fields{})
}

var NopFinishExec = []string{"/bin/true"} // type must match preparerconfig

type Factory interface {
	NewUUIDPod(id types.PodID, uniqueKey types.PodUniqueKey) (*Pod, error)
	NewLegacyPod(id types.PodID) *Pod
}

type HookFactory interface {
	NewHookPod(id types.PodID) *Pod
}

type factory struct {
	podRoot string
	node    types.NodeName

	fetcher     uri.Fetcher
	requireFile string
}

type hookFactory struct {
	hookRoot string
	node     types.NodeName
	fetcher  uri.Fetcher
}

func NewFactory(podRoot string, node types.NodeName, fetcher uri.Fetcher, requireFile string) Factory {
	if podRoot == "" {
		podRoot = DefaultPath
	}

	return &factory{
		podRoot:     podRoot,
		node:        node,
		fetcher:     fetcher,
		requireFile: requireFile,
	}
}

func NewHookFactory(hookRoot string, node types.NodeName, fetcher uri.Fetcher) HookFactory {
	if hookRoot == "" {
		hookRoot = filepath.Join(DefaultPath, "hooks")
	}

	return &hookFactory{
		hookRoot: hookRoot,
		node:     node,
		fetcher:  fetcher,
	}
}

func computeUniqueName(id types.PodID, uniqueKey types.PodUniqueKey) string {
	name := id.String()
	if uniqueKey != "" {
		// If the pod was scheduled with a UUID, we want to namespace its pod home
		// with the same uuid. This enables multiple pods with the same pod ID to
		// exist on the same filesystem
		name = fmt.Sprintf("%s-%s", name, uniqueKey)
	}

	return name
}

func (f *factory) NewUUIDPod(id types.PodID, uniqueKey types.PodUniqueKey) (*Pod, error) {
	if uniqueKey == "" {
		return nil, util.Errorf("uniqueKey cannot be empty")
	}
	home := filepath.Join(f.podRoot, computeUniqueName(id, uniqueKey))
	return newPodWithHome(id, uniqueKey, home, f.node, f.requireFile, f.fetcher), nil
}

func (f *factory) NewLegacyPod(id types.PodID) *Pod {
	home := filepath.Join(f.podRoot, id.String())
	return newPodWithHome(id, "", home, f.node, f.requireFile, f.fetcher)
}

func (f *hookFactory) NewHookPod(id types.PodID) *Pod {
	home := filepath.Join(f.hookRoot, id.String())

	// Hooks can't have a UUID
	return newPodWithHome(id, "", home, f.node, "", f.fetcher)
}

func newPodWithHome(id types.PodID, uniqueKey types.PodUniqueKey, podHome string, node types.NodeName, requireFile string, fetcher uri.Fetcher) *Pod {
	var logger logging.Logger
	logger = Log.SubLogger(logrus.Fields{"pod": id, "uuid": uniqueKey})

	if fetcher == nil {
		fetcher = uri.DefaultFetcher
	}

	return &Pod{
		Id:             id,
		uniqueKey:      uniqueKey,
		home:           podHome,
		node:           node,
		logger:         logger,
		SV:             runit.DefaultSV,
		ServiceBuilder: runit.DefaultBuilder,
		P2Exec:         p2exec.DefaultP2Exec,
		DefaultTimeout: 60 * time.Second,
		LogExec:        runit.DefaultLogExec(),
		FinishExec:     NopFinishExec,
		Fetcher:        fetcher,
		RequireFile:    requireFile,
	}
}

func PodFromPodHome(node types.NodeName, home string) (*Pod, error) {
	return PodFromPodHomeWithReqFile(node, home, "")
}

func PodFromPodHomeWithReqFile(node types.NodeName, home string, requireFile string) (*Pod, error) {
	// Check if the pod home is namespaced by a UUID and pass it to newPodWithHome
	// uniqueKey can be nil if pod home is not namespaced by a UUID
	var uniqueKey types.PodUniqueKey
	podUUID := types.HomeToPodUUID(home)
	if podUUID != nil {
		uniqueKey = types.PodUniqueKey(podUUID.String())
	}

	temp := Pod{
		home: home,
		node: node,
	}
	manifest, err := temp.CurrentManifest()
	if err == NoCurrentManifest {
		return nil, util.Errorf("No current manifest set, this is not an extant pod directory")
	} else if err != nil {
		return nil, err
	}

	// TODO: Shouldn't the Fetcher be configured? So one should get passed in, right?
	return newPodWithHome(manifest.ID(), uniqueKey, home, node, requireFile, nil), nil
}
