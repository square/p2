// +build !race

package dsstore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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

func TestDisableWithAudit(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	logger := logging.TestLogger()
	dsStore := NewConsul(fixture.Client, 0, &logger)
	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())

	auditingStore := NewAuditingStore(dsStore, auditLogStore)

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	ds, err := dsStore.Create(ctx, testManifest(), 1, "some_name", klabels.Everything(), "some_pod", 0)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	if ds.Disabled {
		t.Fatal("expected daemon set to start out disabled")
	}

	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	_, err = auditingStore.Disable(ctx, ds.ID, "some_user")
	if err != nil {
		t.Fatal(err)
	}

	if ds.Disabled {
		t.Fatal("daemon set should not have been disabled before the transaction was committed")
	}

	// confirm no audit log records exist yet
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

	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !ds.Disabled {
		t.Fatal("daemon set should have been disabled after the transaction was committed")
	}

	alMap, err = auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(alMap) != 1 {
		t.Errorf("expected 1 audit log after committing transaction but there were %d", len(alMap))
	}

	for _, v := range alMap {
		if v.EventType != audit.DSDisabledEvent {
			t.Errorf("expected audit log record with type %q but was %q", audit.DSDisabledEvent, v.EventType)
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

		if !details.DaemonSet.Disabled {
			t.Errorf("expected daemon set in audit log record to have been disabled (i.e. the daemon set AFTER the operation is what shows up in audit log)")
		}
	}
}

func TestEnableWithAudit(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	logger := logging.TestLogger()
	dsStore := NewConsul(fixture.Client, 0, &logger)
	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())

	auditingStore := NewAuditingStore(dsStore, auditLogStore)

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	ds, err := dsStore.Create(ctx, testManifest(), 1, "some_name", klabels.Everything(), "some_pod", 0)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	_, err = dsStore.DisableTxn(ctx, ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !ds.Disabled {
		t.Fatal("daemon set should have been disabled after the transaction was committed")
	}

	ctx, cancel = transaction.New(context.Background())
	defer cancel()

	_, err = auditingStore.Enable(ctx, ds.ID, "some_user")
	if err != nil {
		t.Fatal(err)
	}

	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !ds.Disabled {
		t.Fatal("daemon set should not have been enabled before the transaction was committed")
	}

	// confirm no audit log records were created yet
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

	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if ds.Disabled {
		t.Fatal("daemon set should have been enabled after the transaction was committed")
	}

	alMap, err = auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(alMap) != 1 {
		t.Errorf("expected 1 audit log after committing transaction but there were %d", len(alMap))
	}

	for _, v := range alMap {
		if v.EventType != audit.DSEnabledEvent {
			t.Errorf("expected audit log record with type %q but was %q", audit.DSEnabledEvent, v.EventType)
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

		if details.DaemonSet.Disabled {
			t.Errorf("expected daemon set in audit log record to have been enabled (i.e. the daemon set AFTER the operation is what shows up in audit log)")
		}
	}
}

func TestUpdateManifest(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	logger := logging.TestLogger()
	dsStore := NewConsul(fixture.Client, 0, &logger)
	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())

	auditingStore := NewAuditingStore(dsStore, auditLogStore)

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	ds, err := dsStore.Create(ctx, testManifest(), 1, "some_name", klabels.Everything(), "some_pod", 0)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	builder := manifest.NewBuilder()
	builder.SetID("some_other_pod_id")
	podManifestWithDifferentPodID := builder.GetManifest()

	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	_, err = auditingStore.UpdateManifest(ctx, ds.ID, podManifestWithDifferentPodID, "some_user")
	if err == nil {
		t.Error("expected an error when changing the pod ID of the manifest")
	}

	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	builder.SetID("some_pod")
	err = builder.SetConfig(map[interface{}]interface{}{"foo": "bar"})
	if err != nil {
		t.Fatal(err)
	}

	newManifest := builder.GetManifest()

	_, err = auditingStore.UpdateManifest(ctx, ds.ID, newManifest, "some_user")
	if err != nil {
		t.Fatal(err)
	}

	// confirm the manifest hasn't changed yet and also there's no audit log record
	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	dsSHA, err := ds.Manifest.SHA()
	if err != nil {
		t.Fatal(err)
	}

	testSHA, err := testManifest().SHA()
	if err != nil {
		t.Fatal(err)
	}

	if testSHA != dsSHA {
		t.Error("the daemon set's manifest shouldn't have changed before committing the transaction")
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

	// confirm the manifest has now changed
	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	dsSHA, err = ds.Manifest.SHA()
	if err != nil {
		t.Fatal(err)
	}

	newManifestSHA, err := newManifest.SHA()
	if err != nil {
		t.Fatal(err)
	}

	alMap, err = auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(alMap) != 1 {
		t.Errorf("expected 1 audit log before committing transaction but there were %d", len(alMap))
	}

	for _, v := range alMap {
		if v.EventType != audit.DSManifestUpdatedEvent {
			t.Errorf("expected audit log record with type %q but was %q", audit.DSManifestUpdatedEvent, v.EventType)
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

		manifestSHA, err := details.DaemonSet.Manifest.SHA()
		if err != nil {
			t.Fatal(err)
		}

		if manifestSHA != newManifestSHA {
			t.Error("the manifest in the audit log record didn't reflect the manifest change")
		}
	}
}

func TestUpdateNodeSelector(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	logger := logging.TestLogger()
	dsStore := NewConsul(fixture.Client, 0, &logger)
	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())

	auditingStore := NewAuditingStore(dsStore, auditLogStore)

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	ds, err := dsStore.Create(ctx, testManifest(), 1, "some_name", klabels.Everything(), "some_pod", 0)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = transaction.New(context.Background())
	defer cancel()

	newSelector := klabels.Everything().Add("whatever key", klabels.EqualsOperator, []string{"whatever value"})
	_, err = auditingStore.UpdateNodeSelector(ctx, ds.ID, newSelector, "some_user")
	if err != nil {
		t.Fatal(err)
	}

	// confirm the node selector hasn't changed yet and also there's no audit log record
	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if ds.NodeSelector.String() != klabels.Everything().String() {
		t.Error("the daemon set's node selector shouldn't have changed before committing the transaction")
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

	// confirm the manifest has now changed
	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if ds.NodeSelector.String() != newSelector.String() {
		t.Error("the daemon set's node selector should have changed after committing the transaction")
	}

	alMap, err = auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(alMap) != 1 {
		t.Errorf("expected 1 audit log before committing transaction but there were %d", len(alMap))
	}

	for _, v := range alMap {
		if v.EventType != audit.DSNodeSelectorUpdatedEvent {
			t.Errorf("expected audit log record with type %q but was %q", audit.DSNodeSelectorUpdatedEvent, v.EventType)
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

		if details.DaemonSet.NodeSelector.String() != newSelector.String() {
			t.Errorf("expected the audit log record to have the selector changed to %q but was %q", newSelector, details.DaemonSet.NodeSelector)
		}
	}
}

func TestDeleteWithAudit(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	logger := logging.TestLogger()
	dsStore := NewConsul(fixture.Client, 0, &logger)
	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())

	auditingStore := NewAuditingStore(dsStore, auditLogStore)

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	ds, err := dsStore.Create(ctx, testManifest(), 1, "some_name", klabels.Everything(), "some_pod", 0)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = transaction.New(context.Background())
	defer cancel()

	err = auditingStore.Delete(ctx, ds.ID, "some_user")
	if err != nil {
		t.Fatal(err)
	}

	// confirm he daemon set wasn't deleted yet
	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
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

	// confirm the daemon set was deleted
	_, _, err = dsStore.Get(ds.ID)
	switch {
	case err == NoDaemonSet:
	case err != nil:
		t.Fatal(err)
	}

	alMap, err = auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(alMap) != 1 {
		t.Errorf("expected 1 audit log before committing transaction but there were %d", len(alMap))
	}

	for _, v := range alMap {
		if v.EventType != audit.DSDeletedEvent {
			t.Errorf("expected audit log record with type %q but was %q", audit.DSDeletedEvent, v.EventType)
		}

		var details audit.DSEventDetails
		err = json.Unmarshal([]byte(*v.EventDetails), &details)
		if err != nil {
			t.Fatal(err)
		}

		if details.User != "some_user" {
			t.Errorf("expected user name on audit record to be %q but was %q", "some_user", details.User)
		}

		// smoke test that the details have the daemon set prior to deletion by checking the ID
		if details.DaemonSet.ID != ds.ID {
			t.Errorf("expected daemon set in audit log record to have ID %s but was %s", ds.ID, details.DaemonSet.ID)
		}
	}
}

func TestCantDeleteMissingDaemonSetWithAudit(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	logger := logging.TestLogger()
	dsStore := NewConsul(fixture.Client, 0, &logger)
	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())

	auditingStore := NewAuditingStore(dsStore, auditLogStore)

	ctx, cancel := transaction.New(context.Background())
	defer cancel()

	err := auditingStore.Delete(ctx, "some_non_existent_ds_id", "some_user")
	if err == nil {
		t.Fatal("expected an error when deleting a non existent daemon set")
	}
}

func TestUpdateMinHealth(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	logger := logging.TestLogger()
	dsStore := NewConsul(fixture.Client, 0, &logger)
	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())

	auditingStore := NewAuditingStore(dsStore, auditLogStore)

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	ds, err := dsStore.Create(ctx, testManifest(), 1, "some_name", klabels.Everything(), "some_pod", 0)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = transaction.New(context.Background())
	defer cancel()

	_, err = auditingStore.UpdateMinHealth(ctx, ds.ID, 23, "some_user")
	if err != nil {
		t.Fatal(err)
	}

	// confirm he daemon set didn't have its min health changed yet
	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if ds.MinHealth != 1 {
		t.Fatal("min health shouldn't have changed prior to committing the transaction")
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

	// confirm the daemon set was deleted
	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if ds.MinHealth != 23 {
		t.Fatal("min health should have been updated after comitting the transaction")
	}

	alMap, err = auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(alMap) != 1 {
		t.Errorf("expected 1 audit log before committing transaction but there were %d", len(alMap))
	}

	for _, v := range alMap {
		if v.EventType != audit.DSModifiedEvent {
			t.Errorf("expected audit log record with type %q but was %q", audit.DSModifiedEvent, v.EventType)
		}

		var details audit.DSEventDetails
		err = json.Unmarshal([]byte(*v.EventDetails), &details)
		if err != nil {
			t.Fatal(err)
		}

		if details.User != "some_user" {
			t.Errorf("expected user name on audit record to be %q but was %q", "some_user", details.User)
		}

		// smoke test that the details have the daemon set prior to deletion by checking the ID
		if details.DaemonSet.ID != ds.ID {
			t.Errorf("expected daemon set in audit log record to have ID %s but was %s", ds.ID, details.DaemonSet.ID)
		}

		if details.DaemonSet.MinHealth != 23 {
			t.Errorf("expected daemon set in audit log record to have %d min health but was %d", 23, details.DaemonSet.MinHealth)
		}
	}
}

func TestUpdateTimeout(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	logger := logging.TestLogger()
	dsStore := NewConsul(fixture.Client, 0, &logger)
	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())

	auditingStore := NewAuditingStore(dsStore, auditLogStore)

	ctx, cancel := transaction.New(context.Background())
	defer cancel()
	ds, err := dsStore.Create(ctx, testManifest(), 1, "some_name", klabels.Everything(), "some_pod", 0)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel = transaction.New(context.Background())
	defer cancel()

	_, err = auditingStore.UpdateTimeout(ctx, ds.ID, 18*time.Second, "some_user")
	if err != nil {
		t.Fatal(err)
	}

	// confirm he daemon set didn't have its timeout changed yet
	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if ds.Timeout != 0 {
		t.Fatal("timeout shouldn't have changed prior to committing the transaction")
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

	// confirm the daemon set was deleted
	ds, _, err = dsStore.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if ds.Timeout != 18*time.Second {
		t.Fatal("timeout should have been updated after comitting the transaction")
	}

	alMap, err = auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(alMap) != 1 {
		t.Errorf("expected 1 audit log before committing transaction but there were %d", len(alMap))
	}

	for _, v := range alMap {
		if v.EventType != audit.DSModifiedEvent {
			t.Errorf("expected audit log record with type %q but was %q", audit.DSModifiedEvent, v.EventType)
		}

		var details audit.DSEventDetails
		err = json.Unmarshal([]byte(*v.EventDetails), &details)
		if err != nil {
			t.Fatal(err)
		}

		if details.User != "some_user" {
			t.Errorf("expected user name on audit record to be %q but was %q", "some_user", details.User)
		}

		// smoke test that the details have the daemon set prior to deletion by checking the ID
		if details.DaemonSet.ID != ds.ID {
			t.Errorf("expected daemon set in audit log record to have ID %s but was %s", ds.ID, details.DaemonSet.ID)
		}

		if details.DaemonSet.Timeout != 18*time.Second {
			t.Errorf("expected daemon set in audit log record to have %s timeout but was %s", 18*time.Second, details.DaemonSet.Timeout)
		}
	}
}

func testManifest() manifest.Manifest {
	builder := manifest.NewBuilder()
	builder.SetID("some_pod")
	return builder.GetManifest()
}
