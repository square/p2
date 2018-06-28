package client

import (
	"reflect"
	"testing"

	scheduler_protos "github.com/square/p2/pkg/grpc/scheduler/protos"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	klabels "k8s.io/kubernetes/pkg/labels"
)

type recordingClient struct {
	shouldErr bool

	// eligibleNodes is the canned return value for EligibleNodes() calls
	eligibleNodes      []types.NodeName
	eligibleNodesCalls []*scheduler_protos.EligibleNodesRequest

	// allocatedNodes is the canned return value for AllocateNodes() calls
	allocatedNodes     []types.NodeName
	allocateNodesCalls []*scheduler_protos.AllocateNodesRequest

	// deallocatedNodes is the canned return value for DeallocateNodes() calls
	deallocatedNodes     []types.NodeName
	deallocateNodesCalls []*scheduler_protos.DeallocateNodesRequest
}

func (r *recordingClient) EligibleNodes(ctx context.Context, in *scheduler_protos.EligibleNodesRequest, opts ...grpc.CallOption) (*scheduler_protos.EligibleNodesResponse, error) {
	r.eligibleNodesCalls = append(r.eligibleNodesCalls, in)
	if r.shouldErr {
		return new(scheduler_protos.EligibleNodesResponse), util.Errorf("i had a programmed error")
	}

	resp := new(scheduler_protos.EligibleNodesResponse)
	for _, node := range r.eligibleNodes {
		resp.EligibleNodes = append(resp.EligibleNodes, node.String())
	}

	return resp, nil
}

func (r *recordingClient) AllocateNodes(ctx context.Context, in *scheduler_protos.AllocateNodesRequest, opts ...grpc.CallOption) (*scheduler_protos.AllocateNodesResponse, error) {
	r.allocateNodesCalls = append(r.allocateNodesCalls, in)
	if r.shouldErr {
		return new(scheduler_protos.AllocateNodesResponse), util.Errorf("i had a programmed error")
	}

	resp := new(scheduler_protos.AllocateNodesResponse)
	for _, node := range r.allocatedNodes {
		resp.AllocatedNodes = append(resp.AllocatedNodes, node.String())
	}

	return resp, nil
}

func (r *recordingClient) DeallocateNodes(ctx context.Context, in *scheduler_protos.DeallocateNodesRequest, opts ...grpc.CallOption) (*scheduler_protos.DeallocateNodesResponse, error) {
	r.deallocateNodesCalls = append(r.deallocateNodesCalls, in)
	if r.shouldErr {
		return new(scheduler_protos.DeallocateNodesResponse), util.Errorf("i had a programmed error")
	}

	return new(scheduler_protos.DeallocateNodesResponse), nil
}

func TestEligibleNodesHappy(t *testing.T) {
	programmedNodes := []types.NodeName{
		"node1",
		"node5000",
	}
	inner := &recordingClient{
		shouldErr:     false,
		eligibleNodes: programmedNodes,
	}
	client := Client{
		schedulerClient: inner,
	}

	selector := klabels.Everything().Add("foo", klabels.EqualsOperator, []string{"bar"})
	nodes, err := client.EligibleNodes(testManifest(), selector)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(nodes, programmedNodes) {
		t.Fatalf("expected node list to be %s but was %s", programmedNodes, nodes)
	}

	if len(inner.eligibleNodesCalls) != 1 {
		t.Fatalf("expected EligibleNodes() to be called once but was called %d times", len(inner.eligibleNodesCalls))
	}

	call := inner.eligibleNodesCalls[0]
	manifest, err := manifest.FromBytes([]byte(call.Manifest))
	if err != nil {
		t.Fatal(err)
	}

	manifestSHA, err := manifest.SHA()
	if err != nil {
		t.Fatal(err)
	}
	expectedSHA, err := testManifest().SHA()
	if err != nil {
		t.Fatal(err)
	}

	if expectedSHA != manifestSHA {
		t.Errorf("expected manifest in call to have sha %q but was %q", expectedSHA, manifestSHA)
	}

	if call.NodeSelector != selector.String() {
		t.Errorf("expected node selector in call to be %q but was %q", selector, call.NodeSelector)
	}
}

func TestEligibleNodesServerError(t *testing.T) {
	inner := &recordingClient{
		shouldErr: true,
	}
	client := Client{
		schedulerClient: inner,
	}

	_, err := client.EligibleNodes(testManifest(), klabels.Everything().Add("foo", klabels.EqualsOperator, []string{"bar"}))
	if err == nil {
		t.Fatal("expected an error when the server fails")
	}
}

func TestAllocateNodesHappy(t *testing.T) {
	programmedNodes := []types.NodeName{
		"node1",
		"node5000",
	}
	inner := &recordingClient{
		shouldErr:      false,
		allocatedNodes: programmedNodes,
	}
	client := Client{
		schedulerClient: inner,
	}

	selector := klabels.Everything().Add("foo", klabels.EqualsOperator, []string{"bar"})
	nodes, err := client.AllocateNodes(testManifest(), selector, 2, false)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(nodes, programmedNodes) {
		t.Fatalf("expected node list to be %s but was %s", programmedNodes, nodes)
	}

	if len(inner.allocateNodesCalls) != 1 {
		t.Fatalf("expected AllocateNodes() to be called once but was called %d times", len(inner.allocateNodesCalls))
	}

	call := inner.allocateNodesCalls[0]
	manifest, err := manifest.FromBytes([]byte(call.Manifest))
	if err != nil {
		t.Fatal(err)
	}

	manifestSHA, err := manifest.SHA()
	if err != nil {
		t.Fatal(err)
	}
	expectedSHA, err := testManifest().SHA()
	if err != nil {
		t.Fatal(err)
	}

	if expectedSHA != manifestSHA {
		t.Errorf("expected manifest in call to have sha %q but was %q", expectedSHA, manifestSHA)
	}

	if call.NodeSelector != selector.String() {
		t.Errorf("expected node selector in call to be %q but was %q", selector, call.NodeSelector)
	}

	if call.NodesRequested != 2 {
		t.Errorf("expected nodes requested count in call to be %d but was %d", 2, call.NodesRequested)
	}
}

func TestAllocatedNodesServerError(t *testing.T) {
	inner := &recordingClient{
		shouldErr: true,
	}
	client := Client{
		schedulerClient: inner,
	}

	_, err := client.AllocateNodes(testManifest(), klabels.Everything().Add("foo", klabels.EqualsOperator, []string{"bar"}), 3, false)
	if err == nil {
		t.Fatal("expected an error when the server fails")
	}
}

func TestDeallocateNodesHappy(t *testing.T) {
	nodesReleased := []types.NodeName{
		"node1",
		"node5000",
	}
	inner := &recordingClient{
		shouldErr: false,
	}
	client := Client{
		schedulerClient: inner,
	}

	selector := klabels.Everything().Add("foo", klabels.EqualsOperator, []string{"bar"})
	err := client.DeallocateNodes(selector, nodesReleased)
	if err != nil {
		t.Fatal(err)
	}

	if len(inner.deallocateNodesCalls) != 1 {
		t.Fatalf("expected AllocateNodes() to be called once but was called %d times", len(inner.deallocateNodesCalls))
	}

	call := inner.deallocateNodesCalls[0]

	if call.NodeSelector != selector.String() {
		t.Errorf("expected node selector in call to be %q but was %q", selector, call.NodeSelector)
	}

	nodesReleasedString := make([]string, len(nodesReleased))
	for i, nodeName := range nodesReleased {
		nodesReleasedString[i] = nodeName.String()
	}

	if !reflect.DeepEqual(call.NodesReleased, nodesReleasedString) {
		t.Errorf("expected nodes released in call to be %s but was %s", nodesReleased, call.NodesReleased)
	}
}

func TestDeallocateNodesServerError(t *testing.T) {
	inner := &recordingClient{
		shouldErr: true,
	}
	client := Client{
		schedulerClient: inner,
	}

	_, err := client.AllocateNodes(testManifest(), klabels.Everything().Add("foo", klabels.EqualsOperator, []string{"bar"}), 3, false)
	if err == nil {
		t.Fatal("expected an error when the server fails")
	}
}
func testManifest() manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID("some_pod_id")
	return builder.GetManifest()
}
