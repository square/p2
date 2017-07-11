// +build !race

package dsstore

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/store/consul/auditlogstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"

	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestCreateWithAudit(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	logger := logging.TestLogger()
	dsStore := NewConsul(fixture.Client, 0, &logger)
	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())

	auditingStore := NewAuditingStore(dsStore, auditLogStore)

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	ds, err := auditingStore.Create(ctx, testManifest(), 1, "some_name", klabels.Everything(), "some_pod", 0, "some_user")
	if err != nil {
		t.Fatal(err)
	}

	// confirm no daemon sets or audit logs exist yet
	dsList, err := dsStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(dsList) != 0 {
		t.Errorf("expected 0 daemon sets before committing transaction but there were %d", len(dsList))
	}

	alMap, err := auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(alMap) != 0 {
		t.Errorf("expected 0 audit logs before committing transaction but there were %d", len(alMap))
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	// confirm there's a daemon set and an audit record
	dsList, err = dsStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(dsList) != 1 {
		t.Errorf("expected 0 daemon set after committing transaction but there were %d", len(dsList))
	}

	alMap, err = auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(alMap) != 1 {
		t.Errorf("expected 1 audit log after committing transaction but there were %d", len(alMap))
	}

	for _, v := range alMap {
		if v.EventType != audit.DSCreatedEvent {
			t.Errorf("expected audit log record with type %q but was %q", audit.DSCreatedEvent, v.EventType)
		}

		var details audit.DSEventDetails
		err = json.Unmarshal([]byte(*v.EventDetails), &details)
		if err != nil {
			t.Fatal(err)
		}

		if details.User != "some_user" {
			t.Errorf("expected user name on audit record to be %q but was %q", "some_user", details.User)
		}

		// smoke test the ID matches the details
		if details.DaemonSet.ID != ds.ID {
			t.Errorf("expected daemon set in audit log record to have ID %s but was %s", ds.ID, details.DaemonSet.ID)
		}
	}
}

func testManifest() manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID("some_pod")
	return builder.GetManifest()
}
