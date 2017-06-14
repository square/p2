package rc

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/square/p2/pkg/audit"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	rc_fields "github.com/square/p2/pkg/rc/fields"
	"github.com/square/p2/pkg/store/consul/auditlogstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/types"

	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestAuditingTransaction(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())
	rc := &replicationController{
		RC: rc_fields.RC{
			Manifest: testManifest(),
			PodLabels: klabels.Set{
				pc_fields.AvailabilityZoneLabel: "some_az",
				pc_fields.ClusterNameLabel:      "some_cn",
			},
		},
		auditLogStore: auditLogStore,
	}

	ctx, cancel := rc.newAuditingTransaction(context.Background(), []types.NodeName{"node1", "node2"})
	defer cancel()

	ctx.AddNode("node3")
	ctx.RemoveNode("node2")

	err := ctx.Commit(fixture.Client.KV())
	if err != nil {
		t.Fatalf("could not commit audit log record: %s", err)
	}

	records, err := auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected a single audit log record but there were %d", len(records))
	}

	for _, record := range records {
		if record.EventType != audit.RCRetargetingEvent {
			t.Errorf("expected audit log type to be %q but was %q", audit.RCRetargetingEvent, record.EventType)
		}

		var details audit.RCRetargetingDetails
		err = json.Unmarshal([]byte(*record.EventDetails), &details)
		if err != nil {
			t.Fatal(err)
		}

		if details.PodID != testManifest().ID() {
			t.Errorf("expected audit log details to have pod ID %q but was %q", testManifest().ID(), details.PodID)
		}
		if details.AvailabilityZone != "some_az" {
			t.Errorf("expected audit log details to have availability zone %q but was %q", "some_az", details.AvailabilityZone)
		}
		if details.ClusterName != "some_cn" {
			t.Errorf("expected audit log details to have cluster name %q but was %q", "some_cn", details.ClusterName)
		}

		if len(details.Nodes) != 2 {
			t.Fatalf("expected 2 nodes but there were %d", len(details.Nodes))
		}
		found1 := false
		found3 := false
		for _, node := range details.Nodes {
			switch node {
			case "node1":
				found1 = true
			case "node3":
				found3 = true
			default:
				t.Errorf("found node %s but that wasn't expected", node)
			}
		}

		if !found1 {
			t.Error("expected to find node1 in the list")
		}

		if !found3 {
			t.Error("expected to find node3 in the list")
		}
	}
}
