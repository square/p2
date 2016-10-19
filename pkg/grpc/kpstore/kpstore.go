package kpstore

import (
	"time"

	kp_protos "github.com/square/p2/pkg/grpc/kpstore/protos"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"golang.org/x/net/context"
)

var _ kp_protos.KPStoreServer = &store{}

type innerStore interface {
	SetPod(podPrefix consul.PodPrefix, nodename types.NodeName, manifest manifest.Manifest) (time.Duration, error)
	Pod(podPrefix consul.PodPrefix, nodename types.NodeName, podId types.PodID) (manifest.Manifest, time.Duration, error)
}

type store struct {
	innerStore innerStore
}

func NewServer(innerStore innerStore) store {
	return store{
		innerStore: innerStore,
	}
}

func (s store) SetPod(_ context.Context, req *kp_protos.SetPodRequest) (*kp_protos.SetPodResponse, error) {
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

	return &kp_protos.SetPodResponse{
		Duration: int64(duration),
	}, nil
}

func (s store) GetPod(_ context.Context, req *kp_protos.GetPodRequest) (*kp_protos.GetPodResponse, error) {
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

	return &kp_protos.GetPodResponse{
		Duration: int64(duration),
		Manifest: string(bytes),
	}, nil
}

func getPodPrefix(podPrefix kp_protos.PodPrefix) (consul.PodPrefix, error) {
	// Check that pod prefix was set
	if podPrefix == kp_protos.PodPrefix_unknown {
		return "", util.Errorf("Received empty pod prefix")
	}

	// Check that pod prefix is valid
	if podPrefix.String() == "" {
		return "", util.Errorf("invalid pod prefix %d", podPrefix)
	}

	return consul.PodPrefix(podPrefix.String()), nil
}

func getNodeName(nodeName string) (types.NodeName, error) {
	if nodeName == "" {
		return "", util.Errorf("Received empty node name")
	}

	return types.NodeName(nodeName), nil
}
