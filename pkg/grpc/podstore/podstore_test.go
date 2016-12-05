package podstore

import (
	"testing"

	podstore_protos "github.com/square/p2/pkg/grpc/podstore/protos"
	"github.com/square/p2/pkg/kp/consulutil"
	"github.com/square/p2/pkg/kp/podstore"
	"github.com/square/p2/pkg/types"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestSchedulePod(t *testing.T) {
	fakePodStore, server := setupServerWithFakeStore()

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
	_, server := setupServerWithFakeStore()
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
	_, server := setupServerWithFakeStore()
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

func validManifestString() string {
	return "id: test_app"
}

func setupServerWithFakeStore() (podstore.Store, *store) {
	fakePodStore := podstore.NewConsul(consulutil.NewFakeClient().KV_)
	server := &store{
		innerStore: fakePodStore,
	}

	return fakePodStore, server
}
