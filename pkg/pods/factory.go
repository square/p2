package pods

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/p2exec"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/store"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
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
	NewUUIDPod(id store.PodID, uniqueKey store.PodUniqueKey) (*Pod, error)
	NewLegacyPod(id store.PodID) *Pod
}

type HookFactory interface {
	NewHookPod(id store.PodID) *Pod
}

type factory struct {
	podRoot string
	node    store.NodeName
}

type hookFactory struct {
	hookRoot string
	node     store.NodeName
}

func NewFactory(podRoot string, node store.NodeName) Factory {
	if podRoot == "" {
		podRoot = DefaultPath
	}

	return &factory{
		podRoot: podRoot,
		node:    node,
	}
}

func NewHookFactory(hookRoot string, node store.NodeName) HookFactory {
	if hookRoot == "" {
		hookRoot = filepath.Join(DefaultPath, "hooks")
	}

	return &hookFactory{
		hookRoot: hookRoot,
		node:     node,
	}
}

func computeUniqueName(id store.PodID, uniqueKey store.PodUniqueKey) string {
	name := id.String()
	if uniqueKey != "" {
		// If the pod was scheduled with a UUID, we want to namespace its pod home
		// with the same uuid. This enables multiple pods with the same pod ID to
		// exist on the same filesystem
		name = fmt.Sprintf("%s-%s", name, uniqueKey)
	}

	return name
}

func (f *factory) NewUUIDPod(id store.PodID, uniqueKey store.PodUniqueKey) (*Pod, error) {
	if uniqueKey == "" {
		return nil, util.Errorf("uniqueKey cannot be empty")
	}
	home := filepath.Join(f.podRoot, computeUniqueName(id, uniqueKey))
	return newPodWithHome(id, uniqueKey, home, f.node), nil
}

func (f *factory) NewLegacyPod(id store.PodID) *Pod {
	home := filepath.Join(f.podRoot, id.String())
	return newPodWithHome(id, "", home, f.node)
}

func (f *hookFactory) NewHookPod(id store.PodID) *Pod {
	home := filepath.Join(f.hookRoot, id.String())

	// Hooks can't have a UUID
	return newPodWithHome(id, "", home, f.node)
}

func newPodWithHome(id store.PodID, uniqueKey store.PodUniqueKey, podHome string, node store.NodeName) *Pod {
	var logger logging.Logger
	logger = Log.SubLogger(logrus.Fields{"pod": id, "uuid": uniqueKey})

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
		Fetcher:        uri.DefaultFetcher,
	}
}

func PodFromPodHome(node store.NodeName, home string) (*Pod, error) {
	// Check if the pod home is namespaced by a UUID by splitting on a hyphen and
	// checking the last part. If it parses as a UUID, pass it to newPodWithHome.
	// Otherwise, pass a nil uniqueKey
	homeParts := strings.Split(filepath.Base(home), "-")

	var uniqueKey store.PodUniqueKey
	podUUID := uuid.Parse(homeParts[len(homeParts)-1])
	if podUUID != nil {
		uniqueKey = store.PodUniqueKey(podUUID.String())
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

	return newPodWithHome(manifest.ID(), uniqueKey, home, node), nil
}
