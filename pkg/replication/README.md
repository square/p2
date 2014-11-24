# Replication: facilities for scheduling a pod across multiple nodes

The intent store is a relatively open book - for maintenance and introspection purposes, it should be modifable by users. This facility allows declarative management of a pod across multiple nodes, with potentially different leader/follower or other configurations.

Replication is managed by clients exclusively. The only state tracked by these utilities are the manifests found in the intent store.

# General usage

```
package mycorp

import (
    "fmt"

    "github.com/square/p2/pkg/allocation"
    "github.com/square/p2/pkg/health"
    "github.com/square/p2/pkg/pods"
    "github.com/square/p2/pkg/intent"
    "github.com/square/p2/pkg/replication"
    "github.com/mycorp/myp2allocator"
)

func LaunchMysqlPod(mysqlManfiest pods.PodManifest, allocator allocation.Allocator, store intent.Store, healthChecker *health.Checker) (replication.Result, error){
    request := allocation.Request {
        Manifest: mysqlManifest,
        Replicas: 3,
    }
    allocated, err := allocator.Allocate(request)
    master := allocated.MasterNode()
    if !master.Valid() {
        return nil, fmt.Errorf("No ste")
    }
    mysqlManifest.Config["master_node"] = master.Name
    if err != nil {
        return nil, fmt.Errorf("Could not allocate for pod %s: %s", mysqlManifest.Id, err)
    }

    replicator := replication.NewReplicator(mysqlManifest, allocated)
    replicator.MinimumNodes = 2
    return replicator.Enact(store, healthChecker)
}
```

This example code allocates and deploys 3 instances of the given MySQL pod to different nodes and updates the pod manifest's configuration.