package store_server

import (
	"time"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/manifest"
	store_protos "github.com/square/p2/pkg/store_server/protos"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"golang.org/x/net/context"
)

var _ store_protos.StoreServer = &store{}

type innerStore interface {
	SetPod(podPrefix kp.PodPrefix, nodename types.NodeName, manifest manifest.Manifest) (time.Duration, error)
	Pod(podPrefix kp.PodPrefix, nodename types.NodeName, podId types.PodID) (manifest.Manifest, time.Duration, error)
}

type store struct {
	innerStore innerStore
}

func NewServer(innerStore innerStore) store {
	return store{
		innerStore: innerStore,
	}
}

func (s store) SetPod(_ context.Context, req *store_protos.SetPodRequest) (*store_protos.SetPodResponse, error) {
	podPrefix, err := getPodPrefix(req.PodPrefix)
	if err != nil {
		return nil, err
	}

	nodeName, err := getNodeName(req.NodeName)
	if err != nil {
		return nil, err
	}

	m, err := manifest.FromString(req.Manifest)
	if err != nil {
		return nil, err
	}

	duration, err := s.innerStore.SetPod(podPrefix, nodeName, m)
	if err != nil {
		return nil, err
	}

	return &store_protos.SetPodResponse{
		Duration: int64(duration),
	}, nil
}

func (s store) GetPod(_ context.Context, req *store_protos.GetPodRequest) (*store_protos.GetPodResponse, error) {
	podPrefix, err := getPodPrefix(req.PodPrefix)
	if err != nil {
		return nil, err
	}

	nodeName, err := getNodeName(req.NodeName)
	if err != nil {
		return nil, err
	}

	if req.PodId == "" {
		return nil, util.Errorf("Received empty pod ID")
	}

	manifest, duration, err := s.innerStore.Pod(podPrefix, nodeName, types.PodID(req.PodId))
	if err != nil {
		return nil, err
	}

	bytes, err := manifest.Marshal()
	if err != nil {
		return nil, err
	}

	return &store_protos.GetPodResponse{
		Duration: int64(duration),
		Manifest: string(bytes),
	}, nil
}

func getPodPrefix(podPrefix store_protos.PodPrefix) (kp.PodPrefix, error) {
	// Check that pod prefix was set
	if podPrefix == store_protos.PodPrefix_unknown {
		return "", util.Errorf("Received empty pod prefix")
	}

	// Check that pod prefix is valid
	if podPrefix.String() == "" {
		return "", util.Errorf("invalid pod prefix %d", podPrefix)
	}

	return kp.PodPrefix(podPrefix.String()), nil
}

func getNodeName(nodeName string) (types.NodeName, error) {
	if nodeName == "" {
		return "", util.Errorf("Received empty node name")
	}

	return types.NodeName(nodeName), nil
}
