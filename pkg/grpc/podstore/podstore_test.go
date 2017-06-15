package podstore

import (
	"testing"
	"time"

	podstore_protos "github.com/square/p2/pkg/grpc/podstore/protos"
	"github.com/square/p2/pkg/grpc/testutil"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/podstore"
	"github.com/square/p2/pkg/store/consul/podstore/podstoretest"
	"github.com/square/p2/pkg/store/consul/statusstore"
	"github.com/square/p2/pkg/store/consul/statusstore/podstatus"
	"github.com/square/p2/pkg/store/consul/statusstore/statusstoretest"
	"github.com/square/p2/pkg/types"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestSchedulePod(t *testing.T) {
	fakePodStore, server := setupServerWithFakePodStore()

	req := &podstore_protos.SchedulePodRequest{
		Manifest: validManifestString(),
		NodeName: "test_node",
	}

	resp, err := server.SchedulePod(context.Background(), req)
	if err != nil {
		t.Fatalf("Unexpected error from SchedulePod: %s", err)
	}

	// check that the key we got back actually returns an entry from the store
	pod, err := fakePodStore.ReadPod(types.PodUniqueKey(resp.PodUniqueKey))
	if err != nil {
		t.Fatalf("Unexpected error reading pod out of store after scheduling: %s", err)
	}

	if pod.Manifest.ID() != "test_app" {
		t.Errorf("Scheduled pod manifest had wrong ID, expected %q but got %q", "test_app", pod.Manifest.ID())
	}

	if pod.Node != "test_node" {
		t.Errorf("Scheduled node didn't match expectation, expected %q but got %q", "test_node", pod.Node)
	}
}

func TestSchedulePodFailsNoNodeName(t *testing.T) {
	_, server := setupServerWithFakePodStore()
	req := &podstore_protos.SchedulePodRequest{
		Manifest: validManifestString(),
		NodeName: "", // MISSING
	}

	_, err := server.SchedulePod(context.Background(), req)
	if err == nil {
		t.Fatal("Expected an error when the request is missing node name, but didn't get one")
	}

	if grpc.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected error to be %s but was %s", codes.InvalidArgument.String(), grpc.ErrorDesc(err))
	}
}

func TestSchedulePodFailsNoManifest(t *testing.T) {
	_, server := setupServerWithFakePodStore()
	req := &podstore_protos.SchedulePodRequest{
		Manifest: "", // MISSING
		NodeName: "test_node",
	}

	_, err := server.SchedulePod(context.Background(), req)
	if err == nil {
		t.Fatal("Expected an error when the request is missing manifest, but didn't get one")
	}

	if grpc.Code(err) != codes.InvalidArgument {
		t.Errorf("Expected error to be %s but was %s", codes.InvalidArgument.String(), grpc.ErrorDesc(err))
	}
}

func TestUnschedulePod(t *testing.T) {
	store, server := setupServerWithFakePodStore()

	key, err := store.Schedule(validManifest(), "some_node")
	if err != nil {
		t.Fatalf("could not seed pod store with a pod to unschedule: %s", err)
	}

	req := &podstore_protos.UnschedulePodRequest{
		PodUniqueKey: key.String(),
	}
	resp, err := server.UnschedulePod(context.Background(), req)
	if err != nil {
		t.Errorf("unexpected error unscheduling pod: %s", err)
	}

	if resp == nil {
		t.Error("expected non-nil response on successful pod unschedule")
	}
}

func TestUnschedulePodNotFound(t *testing.T) {
	_, server := setupServerWithFakePodStore()

	// unschedule a random key that doesn't exist in the store
	req := &podstore_protos.UnschedulePodRequest{
		PodUniqueKey: types.NewPodUUID().String(),
	}
	resp, err := server.UnschedulePod(context.Background(), req)
	if err == nil {
		t.Error("expected error unscheduling nonexistent pod")
	}

	if grpc.Code(err) != codes.NotFound {
		t.Errorf("expected not found error when unscheduling a nonexistent pod, but error was %s", err)
	}

	if resp != nil {
		t.Error("expected nil response when attempting to unschedule a nonexistent pod")
	}
}

func TestUnscheduleError(t *testing.T) {
	// Create a server with a failing pod store so we can test what happens when
	// a failure is encountered
	server := store{
		scheduler: podstoretest.NewFailingPodStore(),
	}

	req := &podstore_protos.UnschedulePodRequest{
		PodUniqueKey: types.NewPodUUID().String(),
	}
	resp, err := server.UnschedulePod(context.Background(), req)
	if err == nil {
		t.Fatal("expected an error unscheduling a pod using a failing pod store")
	}

	if grpc.Code(err) != codes.Unavailable {
		t.Fatalf("expected an unavailable error when unscheduling from store fails but got %s", err)
	}

	if resp != nil {
		t.Fatal("expected nil response when encountering an unscheduling error")
	}
}

func validManifestString() string {
	return "id: test_app"
}

func validManifest() manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID("test_app")
	return builder.GetManifest()
}

// implements podstore_protos.PodStore_WatchPodStatusServer
type WatchPodStatusStream struct {
	*testutil.FakeServerStream

	ResponseCh chan *podstore_protos.PodStatusResponse
}

// Records responses sent on the stream
func (w *WatchPodStatusStream) Send(resp *podstore_protos.PodStatusResponse) error {
	w.ResponseCh <- resp
	return nil
}

func TestWatchPodStatus(t *testing.T) {
	respCh := make(chan *podstore_protos.PodStatusResponse)
	stream := &WatchPodStatusStream{
		FakeServerStream: testutil.NewFakeServerStream(context.Background()),
		ResponseCh:       respCh,
	}

	podStatusStore, server := setupServerWithFakePodStatusStore()

	podUniqueKey := types.NewPodUUID()
	req := &podstore_protos.WatchPodStatusRequest{
		StatusNamespace: consul.PreparerPodStatusNamespace.String(),
		PodUniqueKey:    podUniqueKey.String(),
		WaitForExists:   true,
	}

	watchErrCh := make(chan error)
	defer close(watchErrCh)
	go func() {
		err := server.WatchPodStatus(req, stream)
		if err != nil {
			watchErrCh <- err
		}
	}()

	expectedTime := time.Now()
	expectedManifest := `id: "test_app"`
	expectedPodState := podstatus.PodLaunched
	expectedLaunchableID := launch.LaunchableID("nginx")
	expectedEntryPoint := "launch"
	expectedExitCode := 3
	expectedExitStatus := 4
	setStatusErrCh := make(chan error)
	defer close(setStatusErrCh)
	go func() {
		err := podStatusStore.Set(podUniqueKey, podstatus.PodStatus{
			Manifest:  expectedManifest,
			PodStatus: expectedPodState,
			ProcessStatuses: []podstatus.ProcessStatus{
				{
					LaunchableID: expectedLaunchableID,
					EntryPoint:   expectedEntryPoint,
					LastExit: &podstatus.ExitStatus{
						ExitTime:   expectedTime,
						ExitCode:   expectedExitCode,
						ExitStatus: expectedExitStatus,
					},
				},
			},
		})
		if err != nil {
			setStatusErrCh <- err
		}
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Didn't receive value after 5 seconds")
	case err := <-setStatusErrCh:
		t.Fatalf("Error setting status to trigger watch: %s", err)
	case err := <-watchErrCh:
		t.Fatalf("Unexpected error watching for status: %s", err)
	case resp := <-respCh:
		if resp.Manifest != expectedManifest {
			t.Errorf("Manifest didn't match expected, wanted %q got %q", expectedManifest, resp.Manifest)
		}

		if resp.PodState != expectedPodState.String() {
			t.Errorf("PodState didn't match expcted, wanted %q got %q", expectedPodState, resp.PodState)
		}

		if len(resp.ProcessStatuses) != 1 {
			t.Fatalf("Expected 1 process status in pod status but got %d", len(resp.ProcessStatuses))
		}

		processStatus := resp.ProcessStatuses[0]
		if processStatus.LaunchableId != expectedLaunchableID.String() {
			t.Errorf("Expected process status for launchable %q but found %q", expectedLaunchableID, processStatus.LaunchableId)
		}

		if processStatus.EntryPoint != expectedEntryPoint {
			t.Errorf("Expected process status for entry point %q but found %q", expectedEntryPoint, processStatus.EntryPoint)
		}

		if processStatus.LastExit == nil {
			t.Fatal("Expected exit information for process")
		}

		lastExit := processStatus.LastExit
		if lastExit.ExitTime != expectedTime.Unix() {
			t.Error("Exit time for process in status didn't match expected")
		}

		if lastExit.ExitCode != int64(expectedExitCode) {
			t.Errorf("Expected exit code %d but got %d", expectedExitCode, lastExit.ExitCode)
		}

		if lastExit.ExitStatus != int64(expectedExitStatus) {
			t.Errorf("Expected exit status %d but got %d", expectedExitStatus, lastExit.ExitStatus)
		}
	}
}

func TestListPodStatus(t *testing.T) {
	statusStore, server := setupServerWithFakePodStatusStore()
	results, err := server.ListPodStatus(context.Background(), &podstore_protos.ListPodStatusRequest{
		StatusNamespace: consul.PreparerPodStatusNamespace.String(),
	})
	if err != nil {
		t.Errorf("error listing pod status: %s", err)
	}

	if len(results.PodStatuses) != 0 {
		t.Fatalf("expected no results when listing pod status from empty store but got %d", len(results.PodStatuses))
	}

	key := types.NewPodUUID()
	err = statusStore.Set(key, podstatus.PodStatus{
		PodStatus: podstatus.PodLaunched,
	})
	if err != nil {
		t.Fatalf("unable to seed status store with a pod status: %s", err)
	}

	results, err = server.ListPodStatus(context.Background(), &podstore_protos.ListPodStatusRequest{
		StatusNamespace: consul.PreparerPodStatusNamespace.String(),
	})
	if err != nil {
		t.Errorf("error listing pod status: %s", err)
	}

	if len(results.PodStatuses) != 1 {
		t.Fatalf("expected one status record but there were %d", len(results.PodStatuses))
	}

	val, ok := results.PodStatuses[key.String()]
	if !ok {
		t.Fatalf("expected a record for pod %s but there wasn't", key)
	}

	if val.PodState != podstatus.PodLaunched.String() {
		t.Errorf("expected pod status of status record to be %q but was %q", podstatus.PodLaunched, val.PodState)
	}
}

func TestDeletePodStatus(t *testing.T) {
	statusStore, server := setupServerWithFakePodStatusStore()
	key := types.NewPodUUID()
	err := statusStore.Set(key, podstatus.PodStatus{
		PodStatus: podstatus.PodLaunched,
	})
	if err != nil {
		t.Fatalf("unable to seed status store with a pod status: %s", err)
	}

	// confirm that there is one entry
	results, err := server.ListPodStatus(context.Background(), &podstore_protos.ListPodStatusRequest{
		StatusNamespace: consul.PreparerPodStatusNamespace.String(),
	})
	if err != nil {
		t.Errorf("error listing pod status: %s", err)
	}

	if len(results.PodStatuses) != 1 {
		t.Fatalf("expected one status record but there were %d", len(results.PodStatuses))
	}

	// now delete it
	_, err = server.DeletePodStatus(context.Background(), &podstore_protos.DeletePodStatusRequest{
		PodUniqueKey: key.String(),
	})
	if err != nil {
		t.Fatalf("error deleting pod status: %s", err)
	}

	// confirm that there are now no entries
	results, err = server.ListPodStatus(context.Background(), &podstore_protos.ListPodStatusRequest{
		StatusNamespace: consul.PreparerPodStatusNamespace.String(),
	})
	if err != nil {
		t.Errorf("error listing pod status: %s", err)
	}

	if len(results.PodStatuses) != 0 {
		t.Fatalf("expected no status records but there were %d", len(results.PodStatuses))
	}
}

// Tests the convenience function that converts from the protobuf definition of
// pod status to the raw type.
func TestPodStatusResposeToPodStatus(t *testing.T) {
	in := podstore_protos.PodStatusResponse{
		PodState: "removed",
		Manifest: "id: foobar",
		ProcessStatuses: []*podstore_protos.ProcessStatus{
			{
				LaunchableId: "whatever",
				EntryPoint:   "some_entry_point",
				LastExit: &podstore_protos.ExitStatus{
					ExitTime:   10000,
					ExitCode:   24,
					ExitStatus: 1800,
				},
			},
		},
	}

	out := PodStatusResponseToPodStatus(in)
	if out.PodStatus.String() != in.PodState {
		t.Errorf("expected pod status to be %q but was %q", in.PodState, out.PodStatus)
	}

	if out.Manifest != in.Manifest {
		t.Errorf("expected manifest to be %q but was %q", in.Manifest, out.Manifest)
	}

	if len(out.ProcessStatuses) != len(in.ProcessStatuses) {
		t.Fatalf("expected %d process status(es) but got %d", len(in.ProcessStatuses), len(out.ProcessStatuses))
	}

	inPS := in.ProcessStatuses[0]
	outPS := out.ProcessStatuses[0]

	if outPS.LaunchableID.String() != inPS.LaunchableId {
		t.Errorf("expected launchable id to be %q but was %q", inPS.LaunchableId, outPS.LaunchableID)
	}

	if outPS.EntryPoint != inPS.EntryPoint {
		t.Errorf("expected entry point to be %q but was %q", inPS.EntryPoint, outPS.EntryPoint)
	}

	inLE := inPS.LastExit
	outLE := outPS.LastExit

	if outLE == nil {
		t.Fatal("expected non-nil last exit")
	}

	if outLE.ExitTime.Unix() != inLE.ExitTime {
		t.Errorf("expected last exit time to be %d but was %d", inLE.ExitTime, outLE.ExitTime.Unix())
	}

	if outLE.ExitCode != int(inLE.ExitCode) {
		t.Errorf("expected exit code to be %d but was %d", inLE.ExitCode, outLE.ExitCode)
	}

	if outLE.ExitStatus != int(inLE.ExitStatus) {
		t.Errorf("expected exit status to be %d but was %d", inLE.ExitStatus, outLE.ExitStatus)
	}
}

func TestMarkPodFailed(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	statusStore := podstatus.NewConsul(statusstore.NewConsul(fixture.Client), consul.PreparerPodStatusNamespace)
	server := store{
		podStatusStore: statusStore,
		consulClient:   fixture.Client,
	}

	key := types.NewPodUUID()
	err := statusStore.Set(key, podstatus.PodStatus{
		PodStatus: podstatus.PodLaunched,
	})
	if err != nil {
		t.Fatalf("unable to seed status store with a pod status: %s", err)
	}

	// mark it as failed
	_, err = server.MarkPodFailed(context.Background(), &podstore_protos.MarkPodFailedRequest{
		PodUniqueKey: key.String(),
	})
	if err != nil {
		t.Fatalf("error marking pod failed: %s", err)
	}

	// confirm that the record is now failed
	resp, err := server.ListPodStatus(context.Background(), &podstore_protos.ListPodStatusRequest{
		StatusNamespace: consul.PreparerPodStatusNamespace.String(),
	})
	if err != nil {
		t.Fatalf("could not list pod status to confirm marking pod as failed: %s", err)
	}

	val, ok := resp.PodStatuses[key.String()]
	if !ok {
		t.Fatal("no status record found for the pod we marked failed")
	}

	if val.PodState != podstatus.PodFailed.String() {
		t.Errorf("expected pod to be marked %q but was %q", podstatus.PodFailed, val.PodState)
	}
}

func setupServerWithFakePodStore() (podstore.Store, store) {
	fakePodStore := podstore.NewConsul(consulutil.NewFakeClient().KV_)
	server := store{
		scheduler: fakePodStore,
	}

	return fakePodStore, server
}

// augment PodStatusStore interface so we can do some test setup
type testPodStatusStore interface {
	PodStatusStore
	Set(key types.PodUniqueKey, status podstatus.PodStatus) error
}

func setupServerWithFakePodStatusStore() (testPodStatusStore, store) {
	fakePodStatusStore := podstatus.NewConsul(statusstoretest.NewFake(), consul.PreparerPodStatusNamespace)
	return fakePodStatusStore, store{
		podStatusStore: fakePodStatusStore,
	}
}
