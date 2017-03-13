package daemonsetstore

import (
	"testing"
	"time"

	"github.com/square/p2/pkg/ds/fields"
	daemonsetstore_protos "github.com/square/p2/pkg/grpc/daemonsetstore/protos"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul/dsstore/dsstoretest"
	"github.com/square/p2/pkg/types"

	"golang.org/x/net/context"
	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestListDaemonSets(t *testing.T) {
	dsStore := dsstoretest.NewFake()

	minHealth := 18
	name := fields.ClusterName("some_daemon_set")
	nodeSelector, err := klabels.Parse("foo=bar")
	if err != nil {
		t.Fatal(err)
	}
	podID := types.PodID("fooapp")
	timeout := 100 * time.Nanosecond

	seedDS, err := dsStore.Create(
		validManifest(),
		minHealth,
		name,
		nodeSelector,
		podID,
		timeout,
	)
	if err != nil {
		t.Fatalf("could not seed daemon set store with a daemon set")
	}

	server := NewServer(dsStore)
	resp, err := server.ListDaemonSets(context.Background(), &daemonsetstore_protos.ListDaemonSetsRequest{})
	if err != nil {
		t.Fatalf("error listing daemon sets: %s", err)
	}

	if len(resp.DaemonSets) != 1 {
		t.Errorf("expected a single daemon set but there were %d", len(resp.DaemonSets))
	}

	ds := resp.DaemonSets[0]

	if ds.Id != seedDS.ID.String() {
		t.Errorf("expected daemon set ID to be %q but was %q", seedDS.ID, ds.Id)
	}

	if ds.Disabled != seedDS.Disabled {
		t.Errorf("expected daemon set's disabled flag to be %t but was %t", seedDS.Disabled, ds.Disabled)
	}

	expectedStr, err := seedDS.Manifest.Marshal()
	if err != nil {
		t.Fatalf("couldn't marshal seed daemon set's manifest as a string: %s", err)
	}
	if ds.Manifest != string(expectedStr) {
		t.Errorf("expected manifest to be %q but was %q", string(expectedStr), ds.Manifest)
	}

	if ds.MinHealth != 18 {
		t.Errorf("expected min health to be %d but was %d", 18, ds.MinHealth)
	}

	if ds.Name != seedDS.Name.String() {
		t.Errorf("expected daemon set name to be %q but was %q", seedDS.Name.String())
	}

	if ds.NodeSelector != seedDS.NodeSelector.String() {
		t.Errorf("expected node selector to be %q but was %q", seedDS.NodeSelector.String(), ds.NodeSelector)
	}

	if ds.PodId != seedDS.PodID.String() {
		t.Errorf("expected pod ID to be %q but was %q", seedDS.PodID, ds.PodId)
	}

	if ds.Timeout != seedDS.Timeout.Nanoseconds() {
		t.Errorf("expected timeout to be %s but was %s", seedDS.Timeout, time.Duration(ds.Timeout))
	}
}

func validManifest() manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID("fooapp")
	return builder.GetManifest()
}
