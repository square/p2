package pods

import (
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

var DefaultFinishExec = []string{"/bin/true"} // type must match preparerconfig

type Factory interface {
	NewPod(id types.PodID) *Pod
}

type HookFactory interface {
	NewHookPod(id types.PodID) *Pod
}

type factory struct {
	podRoot string
	node    types.NodeName
}

type hookFactory struct {
	hookRoot string
	node     types.NodeName
}

func NewFactory(podRoot string, node types.NodeName) Factory {
	if podRoot == "" {
		podRoot = DefaultPath
	}

	return &factory{
		podRoot: podRoot,
		node:    node,
	}
}

func NewHookFactory(hookRoot string, node types.NodeName) HookFactory {
	if hookRoot == "" {
		hookRoot = filepath.Join(DefaultPath, "hooks")
	}

	return &hookFactory{
		hookRoot: hookRoot,
		node:     node,
	}
}

func (f *factory) NewPod(id types.PodID) *Pod {
	home := filepath.Join(f.podRoot, id.String())

	return newPodWithHome(id, home, f.node)
}

func (f *hookFactory) NewHookPod(id types.PodID) *Pod {
	home := filepath.Join(f.hookRoot, id.String())

	return newPodWithHome(id, home, f.node)
}

func newPodWithHome(id types.PodID, podHome string, node types.NodeName) *Pod {
	return &Pod{
		Id:             id,
		home:           podHome,
		node:           node,
		logger:         Log.SubLogger(logrus.Fields{"pod": id}),
		SV:             runit.DefaultSV,
		ServiceBuilder: runit.DefaultBuilder,
		P2Exec:         p2exec.DefaultP2Exec,
		DefaultTimeout: 60 * time.Second,
		LogExec:        runit.DefaultLogExec(),
		FinishExec:     DefaultFinishExec,
		Fetcher:        uri.DefaultFetcher,
	}
}

func PodFromPodHome(node types.NodeName, home string) (*Pod, error) {
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

	return newPodWithHome(manifest.ID(), home, node), nil
}
