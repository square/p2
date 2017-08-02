package podclusters

import (
	"context"
	"testing"
	"time"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	pcfields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/store/consul/configstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"

	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestCreateOrUpdateConfigForPodCluster(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()

	labeler := labels.NewConsulApplicator(fixture.Client, 0)
	err := labeler.SetLabel(labels.Config, "some_id", "some_key", "some_value")
	if err != nil {
		t.Fatal(err)
	}
	pccStore := NewPodClusterConfigStore(fixture.Client, logging.TestLogger())

	ctx, cancel := transaction.New(context.Background())
	defer cancel()

	v := configstore.Version(0)
	version := &v
	podID := types.PodID("some_pod_id")
	az := pcfields.AvailabilityZone("some_az")
	cn := pcfields.ClusterName("some_cn")

	fields := &configstore.Fields{
		Config: map[interface{}]interface{}{
			"foo": "bar",
		},
		ID: "this should be ignored",
	}
	fields, err = pccStore.CreateOrUpdateConfigForPodCluster(
		ctx,
		podID,
		az,
		cn,
		*fields,
		version,
	)
	if err != nil {
		t.Fatal(err)
	}

	kvps, _, err := fixture.Client.KV().List(configstore.ConfigTree, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvps) != 0 {
		t.Fatal("expected no config entry to be written before the transaction is committed")
	}
	allLabels, err := labeler.ListLabels(labels.Config)
	if err != nil {
		t.Fatal(err)
	}

	// we test for 1 instead of zero because we had to create a bogus label to bypass
	// the failsafe that protects against erroneous "no label" results
	if len(allLabels) != 1 {
		t.Fatal("expected no labels to be written before the transaction is committed")
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	kvps, _, err = fixture.Client.KV().List(configstore.ConfigTree, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvps) != 1 {
		t.Fatalf("expected one config entry to be written after the transaction is committed but there were %d", len(kvps))
	}

	allLabels, err = labeler.ListLabels(labels.Config)
	if err != nil {
		t.Fatal(err)
	}

	// we test for 2 instead of 1 because we had to create a bogus label to bypass
	// the failsafe that protects against erroneous "no label" results
	if len(allLabels) != 2 {
		t.Fatal("expected labels to be written after the transaction is committed")
	}

	// now test that we can retrieve those labels
	sel := klabels.Everything().
		Add(pcfields.PodIDLabel, klabels.EqualsOperator, []string{podID.String()}).
		Add(pcfields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{az.String()}).
		Add(pcfields.ClusterNameLabel, klabels.EqualsOperator, []string{cn.String()})
	labeled, err := pccStore.configStore.FindWhereLabeled(sel)
	if err != nil {
		t.Fatal(err)
	}
	if len(labeled) != 1 {
		t.Fatalf("expected one label match but there were %d", len(labeled))
	}

	if labeled[0].Config["foo"] != "bar" {
		t.Fatalf("config didn't match what was set, wanted key %q to have value %q but was %q", "foo", "bar", labeled[0].Config["foo"])
	}
	cancel()

	// give cleanup routine time to unlock the key after canceling the context
	time.Sleep(50 * time.Millisecond)

	// now try to update it with a stale version and confirm the transaction fails
	fields.Config["foo"] = "bar2"
	ctx, cancel = transaction.New(context.Background())
	defer cancel()
	fields, err = pccStore.CreateOrUpdateConfigForPodCluster(
		ctx,
		podID,
		az,
		cn,
		*fields,
		version,
	)
	if err != nil {
		t.Fatal(err)
	}

	ok, _, err := transaction.Commit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	if ok {
		t.Fatal("expected transaction to fail because the version was stale")
	}

	cancel()

	// give cleanup routine time to unlock the key after canceling the context
	time.Sleep(50 * time.Millisecond)

	// now try again with the right version
	_, version, err = pccStore.configStore.(*configstore.ConsulStore).FetchConfig(fields.ID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = transaction.New(context.Background())
	defer cancel()

	fields, err = pccStore.CreateOrUpdateConfigForPodCluster(
		ctx,
		podID,
		az,
		cn,
		*fields,
		version,
	)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatal(err)
	}

	// now test that we can retrieve those labels
	labeled, err = pccStore.configStore.FindWhereLabeled(sel)
	if err != nil {
		t.Fatal(err)
	}
	if len(labeled) != 1 {
		t.Fatalf("expected one label match but there were %d", len(labeled))
	}

	if labeled[0].Config["foo"] != "bar2" {
		t.Fatalf("config didn't match what was set, wanted key %q to have value %q but was %q", "foo", "bar2", labeled[0].Config["foo"])
	}
}
