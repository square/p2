package daemonsetstore

import (
	"testing"
	"time"

	"github.com/square/p2/pkg/ds/fields"
	daemonsetstore_protos "github.com/square/p2/pkg/grpc/daemonsetstore/protos"
	"github.com/square/p2/pkg/grpc/testutil"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul/dsstore"
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

type TestWatchDaemonSetsStream struct {
	*testutil.FakeServerStream

	responseCh chan<- *daemonsetstore_protos.WatchDaemonSetsResponse
}

func (w TestWatchDaemonSetsStream) Send(resp *daemonsetstore_protos.WatchDaemonSetsResponse) error {
	w.responseCh <- resp
	return nil
}

type fakeDaemonSetWatcher struct {
	resultCh <-chan dsstore.WatchedDaemonSets
}

func newFakeDSWatcher(resultCh <-chan dsstore.WatchedDaemonSets) fakeDaemonSetWatcher {
	return fakeDaemonSetWatcher{
		resultCh: resultCh,
	}
}

func (fakeDaemonSetWatcher) List() ([]fields.DaemonSet, error) { panic("List() not implemented") }
func (fakeDaemonSetWatcher) Disable(id fields.ID) (fields.DaemonSet, error) {
	panic("Disable() not implemented")
}
func (f fakeDaemonSetWatcher) Watch(quitCh <-chan struct{}) <-chan dsstore.WatchedDaemonSets {
	out := make(chan dsstore.WatchedDaemonSets)
	go func() {
		defer close(out)

		for {
			select {
			case <-quitCh:
				return
			case val := <-f.resultCh:
				select {
				case <-quitCh:
					return
				case out <- val:
				}
			}
		}
	}()

	return out
}

func TestWatchDaemonSets(t *testing.T) {
	resultCh := make(chan dsstore.WatchedDaemonSets)
	server := NewServer(newFakeDSWatcher(resultCh))

	respCh := make(chan *daemonsetstore_protos.WatchDaemonSetsResponse)
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	serverExit := make(chan struct{})
	go func() {
		defer close(serverExit)
		err := server.WatchDaemonSets(new(daemonsetstore_protos.WatchDaemonSetsRequest), TestWatchDaemonSetsStream{
			FakeServerStream: testutil.NewFakeServerStream(ctx),
			responseCh:       respCh,
		})
		if err != nil {
			t.Fatal(err)
		}
	}()

	ds, err := createADaemonSet(dsstoretest.NewFake())
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		resultCh <- dsstore.WatchedDaemonSets{
			Created: []*fields.DaemonSet{
				&ds,
			},
		}
	}()

	select {
	case out := <-respCh:
		if out.Error != "" {
			t.Fatalf("expected no error from watch but got %s", out.Error)
		}

		if len(out.Created) != 1 {
			t.Fatalf("expected 1 created daemon set but there were %d", len(out.Created))
		}

		if len(out.Updated) != 0 {
			t.Fatalf("expected 0 updated daemon sets but there were %d", len(out.Updated))
		}

		if len(out.Deleted) != 0 {
			t.Fatalf("expected 0 deleted daemon sets but there were %d", len(out.Deleted))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for output")
	}

	go func() {
		resultCh <- dsstore.WatchedDaemonSets{
			Updated: []*fields.DaemonSet{
				&ds,
			},
		}
	}()

	select {
	case out := <-respCh:
		if out.Error != "" {
			t.Fatalf("expected no error from watch but got %s", out.Error)
		}

		if len(out.Created) != 0 {
			t.Fatalf("expected 0 created daemon sets but there were %d", len(out.Created))
		}

		if len(out.Updated) != 1 {
			t.Fatalf("expected 1 updated daemon set but there were %d", len(out.Updated))
		}

		if len(out.Deleted) != 0 {
			t.Fatalf("expected 0 deleted daemon sets but there were %d", len(out.Deleted))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for output")
	}

	go func() {
		resultCh <- dsstore.WatchedDaemonSets{
			Deleted: []*fields.DaemonSet{
				&ds,
			},
		}
	}()

	select {
	case out := <-respCh:
		if out.Error != "" {
			t.Fatalf("expected no error from watch but got %s", out.Error)
		}

		if len(out.Created) != 0 {
			t.Fatalf("expected 0 created daemon sets but there were %d", len(out.Created))
		}

		if len(out.Updated) != 0 {
			t.Fatalf("expected 0 updated daemon sets but there were %d", len(out.Updated))
		}

		if len(out.Deleted) != 1 {
			t.Fatalf("expected 1 deleted daemon set but there were %d", len(out.Deleted))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for output")
	}

	cancelFunc()
	select {
	case _, ok := <-serverExit:
		if ok {
			t.Fatal("expected server to exit after client cancel")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for server to exit")
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
