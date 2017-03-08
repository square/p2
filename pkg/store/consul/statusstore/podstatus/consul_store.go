package podstatus

import (
	"encoding/json"

	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/store/consul/podstore"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/hashicorp/consul/api"
)

type ConsulStore struct {
	statusStore statusstore.Store

	// The consul implementation statusstore.Store formats keys like
	// /status/<resource-type>/<resource-id>/<namespace>. The namespace
	// portion is useful if multiple subsystems need to record their
	// own view of a resource.
	namespace statusstore.Namespace
}

// TODO: this pod store is coupled with the PodStatus struct, which represents
// the book keeping that the preparer does about a pod. In other words it only
// makes sense if the namespace is consul.PreparerPodStatusNamespace. We should
// probably take namespace out of all these APIs and use that constant instead.
func NewConsul(statusStore statusstore.Store, namespace statusstore.Namespace) ConsulStore {
	return ConsulStore{
		statusStore: statusStore,
		namespace:   namespace,
	}
}

func (c ConsulStore) Get(key types.PodUniqueKey) (PodStatus, *api.QueryMeta, error) {
	if key == "" {
		return PodStatus{}, nil, util.Errorf("Cannot retrieve status for a pod with an empty uuid")
	}

	status, queryMeta, err := c.statusStore.GetStatus(statusstore.POD, statusstore.ResourceID(key), c.namespace)
	if err != nil {
		return PodStatus{}, queryMeta, err
	}

	podStatus, err := statusToPodStatus(status)
	if err != nil {
		return PodStatus{}, queryMeta, err
	}

	return podStatus, queryMeta, nil
}

func (c ConsulStore) WaitForStatus(key types.PodUniqueKey, waitIndex uint64) (PodStatus, *api.QueryMeta, error) {
	if key == "" {
		return PodStatus{}, nil, util.Errorf("Cannot retrieve status for a pod with an empty uuid")
	}

	status, queryMeta, err := c.statusStore.WatchStatus(statusstore.POD, statusstore.ResourceID(key), c.namespace, waitIndex)
	if err != nil {
		return PodStatus{}, queryMeta, err
	}

	podStatus, err := statusToPodStatus(status)
	if err != nil {
		return PodStatus{}, queryMeta, err
	}

	return podStatus, queryMeta, nil
}

func (c ConsulStore) GetStatusFromIndex(index podstore.PodIndex) (PodStatus, *api.QueryMeta, error) {
	return c.Get(index.PodKey)
}

func (c ConsulStore) Set(key types.PodUniqueKey, status PodStatus) error {
	if key == "" {
		return util.Errorf("Could not set status for pod with empty uuid")
	}

	rawStatus, err := podStatusToStatus(status)
	if err != nil {
		return err
	}

	return c.statusStore.SetStatus(statusstore.POD, statusstore.ResourceID(key), c.namespace, rawStatus)
}

func (c ConsulStore) CAS(key types.PodUniqueKey, status PodStatus, modifyIndex uint64) error {
	if key == "" {
		return util.Errorf("Could not set status for pod with empty uuid")
	}

	rawStatus, err := podStatusToStatus(status)
	if err != nil {
		return err
	}

	return c.statusStore.CASStatus(statusstore.POD, statusstore.ResourceID(key), c.namespace, rawStatus, modifyIndex)
}

// Convenience function for only mutating a part of the status structure.
// First, the status is retrieved and the consul ModifyIndex is read. The
// status is then passed to a mutator function, and then the new status is
// written back to consul using a CAS operation, guaranteeing that nothing else
// about the status changed.
func (c ConsulStore) MutateStatus(key types.PodUniqueKey, mutator func(PodStatus) (PodStatus, error)) error {
	var lastIndex uint64
	status, queryMeta, err := c.Get(key)
	switch {
	case statusstore.IsNoStatus(err):
		// We just want to make sure the key doesn't exist when we set it, so
		// use an index of 0
		lastIndex = 0
	case err != nil:
		return err
	default:
		lastIndex = queryMeta.LastIndex
	}

	newStatus, err := mutator(status)
	if err != nil {
		return err
	}

	return c.CAS(key, newStatus, lastIndex)
}

// A helper method for updating the LastExit field of one of the processes in a
// pod. Searches through p.ProcessStatuses for a process matching the
// launchable ID and launchableScriptName, and mutates its LastExit if found.
// If not found, a new process is added.
func (c ConsulStore) SetLastExit(podUniqueKey types.PodUniqueKey, launchableID launch.LaunchableID, entryPoint string, exitStatus ExitStatus) error {
	mutator := func(p PodStatus) (PodStatus, error) {
		for _, processStatus := range p.ProcessStatuses {
			if processStatus.LaunchableID == launchableID && processStatus.EntryPoint == entryPoint {
				processStatus.LastExit = &exitStatus
				return p, nil
			}
		}

		p.ProcessStatuses = append(p.ProcessStatuses, ProcessStatus{
			LaunchableID: launchableID,
			EntryPoint:   entryPoint,
			LastExit:     &exitStatus,
		})
		return p, nil
	}

	return c.MutateStatus(podUniqueKey, mutator)
}

// List lists all of the pod status entries in consul.
func (c ConsulStore) List() (map[types.PodUniqueKey]PodStatus, error) {
	allStatus, err := c.statusStore.GetAllStatusForResourceType(statusstore.POD)
	if err != nil {
		return nil, util.Errorf("could not fetch all status for %s resource type: %s", statusstore.POD, err)
	}

	ret := make(map[types.PodUniqueKey]PodStatus)
	for id, statusMap := range allStatus {
		if status, ok := statusMap[c.namespace]; ok {
			podUniqueKey, err := types.ToPodUniqueKey(id.String())
			if err != nil {
				return nil, util.Errorf("got status record with ID %s that could not be converted to pod unique key: %s", id, err)
			}

			var podStatus PodStatus
			err = json.Unmarshal(status.Bytes(), &podStatus)
			if err != nil {
				return nil, util.Errorf("could not unmarshal status for %s as JSON (raw status=%q): %s", podUniqueKey, string(status.Bytes()), err)
			}
			ret[podUniqueKey] = podStatus
		}
	}

	return ret, nil
}
