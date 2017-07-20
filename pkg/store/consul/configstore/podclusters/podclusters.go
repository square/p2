// podclusters contains code for interacting with pod cluster config in a convenient way

package podclusters

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	pfields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/configstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
	"github.com/pborman/uuid"
	klabels "k8s.io/kubernetes/pkg/labels"
)

type ConfigStore interface {
	FindWhereLabeled(klabels.Selector) ([]*configstore.Fields, error)
	PutConfigTxn(context.Context, configstore.Fields, *configstore.Version) error
	LabelConfigTxn(context.Context, configstore.ID, map[string]string) error
}

type SessionManager interface {
	NewUnmanagedSession(session, name string) consul.Session
}

type PodClusterConfigStore struct {
	configStore    ConfigStore
	logger         logging.Logger
	consulClient   consulutil.ConsulClient
	sessionManager SessionManager
	txner          transaction.Txner
}

func NewPodClusterConfigStore(consulClient consulutil.ConsulClient, logger logging.Logger) PodClusterConfigStore {
	labeler := labels.NewConsulApplicator(consulClient, 0)
	return PodClusterConfigStore{
		configStore:    configstore.NewConsulStore(consulClient.KV(), labeler),
		logger:         logger,
		consulClient:   consulClient,
		sessionManager: consul.NewConsulStore(consulClient),
		txner:          consulClient.KV(),
	}
}

func (pccs *PodClusterConfigStore) CreateOrUpdateConfigForPodCluster(
	ctx context.Context,
	podID types.PodID,
	az pfields.AvailabilityZone,
	cn pfields.ClusterName,
	fields configstore.Fields,
	version *configstore.Version,
) (*configstore.Fields, error) {
	sel := klabels.Everything().
		Add(pfields.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(pfields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{az.String()}).
		Add(pfields.ClusterNameLabel, klabels.EqualsOperator, []string{cn.String()})

	sessions := make(chan string)
	quit := make(chan struct{})
	name := fmt.Sprintf("pod-cluster-config-lock-%s", uuid.New())
	go consulutil.SessionManager(api.SessionEntry{
		Name:      name,
		LockDelay: 5 * time.Second,
		Behavior:  api.SessionBehaviorDelete,
		TTL:       "15s",
	}, pccs.consulClient, sessions, quit, pccs.logger)
	sessionID := <-sessions
	if sessionID == "" {
		close(quit)
		return nil, util.Errorf("could not create session")
	}
	session := pccs.sessionManager.NewUnmanagedSession(sessionID, name)

	lockPath := lockPath(podID, az, cn)
	unlocker, err := session.Lock(lockPath)
	if err != nil {
		close(quit)
		return nil, util.Errorf("could not acquire pod cluster config creation lock: %s", err)
	}

	go func() {
		<-ctx.Done()
		// if this fails it's not a huge deal because the session will die when it is not renewed
		_ = unlocker.Unlock()
		close(quit)
	}()

	// TODO: grab a per-pod-cluster lock here to make sure two concurrent writes don't result in two configs for the same pod cluster
	labeled, err := pccs.configStore.FindWhereLabeled(sel)
	if err != nil {
		return nil, util.Errorf("Got error while fetching labeled objects: %v", err)
	}
	if len(labeled) > 1 {
		fields.ID = labeled[0].ID
		return nil, util.Errorf("More than one pod cluster found for given selectors")
	}

	if len(labeled) == 0 {
		fields.ID = configstore.ID(uuid.New())
		labelsMap := make(map[string]string)
		labelsMap[pfields.PodIDLabel] = podID.String()
		labelsMap[pfields.AvailabilityZoneLabel] = az.String()
		labelsMap[pfields.ClusterNameLabel] = cn.String()

		if err := pccs.configStore.LabelConfigTxn(ctx, fields.ID, labelsMap); err != nil {
			return nil, err
		}
	}

	if err := pccs.configStore.PutConfigTxn(ctx, fields, version); err != nil {
		return nil, err
	}

	err = transaction.Add(ctx, api.KVTxnOp{
		Verb:    api.KVCheckSession,
		Key:     lockPath,
		Session: sessionID,
	})
	if err != nil {
		return nil, err
	}

	return &fields, nil
}

func lockPath(podID types.PodID, az pfields.AvailabilityZone, cn pfields.ClusterName) string {
	return path.Join(consulutil.LOCK_TREE, configstore.ConfigTree, podID.String(), az.String(), cn.String())
}
