package daemonsetstore

import (
	"testing"
	"time"

	"github.com/square/p2/pkg/ds/fields"
	daemonsetstore_protos "github.com/square/p2/pkg/grpc/daemonsetstore/protos"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul/dsstore/dsstoretest"
	"github.com/square/p2/pkg/types"

	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestListDaemonSets(t *testing.T) {
	dsStore := dsstoretest.NewFake()
	seedDS, err := createADaemonSet(dsStore)
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
		t.Errorf("expected daemon set name to be %q but was %q", seedDS.Name.String(), ds.Name)
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

func TestDisableDaemonSet(t *testing.T) {
	dsStore := dsstoretest.NewFake()
	daemonSet, err := createADaemonSet(dsStore)
	if err != nil {
		t.Fatal(err)
	}

	// confirm it starts out as enabled
	if daemonSet.Disabled {
		t.Fatal("daemon set already disabled")
	}

	server := NewServer(dsStore)
	_, err = server.DisableDaemonSet(context.Background(), &daemonsetstore_protos.DisableDaemonSetRequest{
		DaemonSetId: daemonSet.ID.String(),
	})
	if err != nil {
		t.Fatalf("error disabling daemon set: %s", err)
	}

	daemonSet, _, err = dsStore.Get(daemonSet.ID)
	if err != nil {
		t.Fatalf("could not fetch daemon set after disabling it to confirm the disable worked: %s", err)
	}

	if !daemonSet.Disabled {
		t.Error("daemon set wasn't disabled")
	}
}

func TestDisableDaemonSetInvalidArgument(t *testing.T) {
	server := NewServer(dsstoretest.NewFake())

	_, err := server.DisableDaemonSet(context.Background(), &daemonsetstore_protos.DisableDaemonSetRequest{
		DaemonSetId: "bad daemon set ID",
	})
	if err == nil {
		t.Fatal("should have gotten an error passing a malformed daemon set ID to disable")
	}

	if grpc.Code(err) != codes.InvalidArgument {
		t.Errorf("should have gotten an invalid argument error but was %q", err)
	}
}

func TestDisableDaemonSetNotFound(t *testing.T) {
	server := NewServer(dsstoretest.NewFake())

	_, err := server.DisableDaemonSet(context.Background(), &daemonsetstore_protos.DisableDaemonSetRequest{
		DaemonSetId: uuid.New(),
	})
	if err == nil {
		t.Fatal("should have gotten an error passing a malformed daemon set ID to disable")
	}

	if grpc.Code(err) != codes.NotFound {
		t.Errorf("should have gotten a not found error but was %q", err)
	}
}

func validManifest() manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID("fooapp")
	return builder.GetManifest()
}

func createADaemonSet(store *dsstoretest.FakeDSStore) (fields.DaemonSet, error) {
	minHealth := 18
	name := fields.ClusterName("some_daemon_set")
	nodeSelector, err := klabels.Parse("foo=bar")
	if err != nil {
		return fields.DaemonSet{}, err
	}
	podID := types.PodID("fooapp")
	timeout := 100 * time.Nanosecond

	return store.Create(
		validManifest(),
		minHealth,
		name,
		nodeSelector,
		podID,
		timeout,
	)
}
