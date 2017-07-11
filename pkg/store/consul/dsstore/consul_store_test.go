// +build !race

package dsstore

import (
	"context"
	"testing"
	"time"

	"github.com/square/p2/pkg/replication"
	"github.com/square/p2/pkg/util"

	. "github.com/anthonybishopric/gotcha"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/transaction"
	"github.com/square/p2/pkg/types"

	klabels "k8s.io/kubernetes/pkg/labels"
)

func TestCreate(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	store := newStore(fixture.Client.KV())
	createDaemonSet(store, fixture.Client.KV(), t)

	// Create a bad DaemonSet
	podID := types.PodID("")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID("")

	podManifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	if _, err := store.Create(ctx, podManifest, minHealth, clusterName, selector, podID, timeout); err == nil {
		t.Error("Expected create to fail on bad pod id")
	}

	podID = types.PodID("pod_id")
	if _, err := store.Create(ctx, podManifest, minHealth, clusterName, selector, podID, timeout); err == nil {
		t.Error("Expected create to fail on bad manifest pod id")
	}

	manifestBuilder = manifest.NewBuilder()
	manifestBuilder.SetID("different_pod_id")

	podManifest = manifestBuilder.GetManifest()
	if _, err := store.Create(ctx, podManifest, minHealth, clusterName, selector, podID, timeout); err == nil {
		t.Error("Expected create to fail on pod id and manifest pod id mismatch")
	}
}

func createDaemonSet(store *ConsulStore, txner transaction.Txner, t *testing.T) ds_fields.DaemonSet {
	podID := types.PodID("some_pod_id")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)

	manifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	ds, err := store.Create(ctx, manifest, minHealth, clusterName, selector, podID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	err = transaction.MustCommit(ctx, txner)
	if err != nil {
		t.Fatalf("could not commit transaction to create daemon set: %s", err)
	}

	if ds.ID == "" {
		t.Error("daemon set should have an id")
	}

	Assert(t).AreNotEqual(ds.ID, "", "Daemon set should have an id")
	Assert(t).AreNotEqual(ds.PodID, "", "Daemon set should have a pod id")

	Assert(t).AreEqual(ds.PodID, podID, "Daemon set pod id was not set correctly")
	Assert(t).AreEqual(ds.MinHealth, minHealth, "Daemon set minimum health was not set correctly")
	Assert(t).AreEqual(ds.Name, clusterName, "Daemon set cluster name was not set correctly")
	Assert(t).IsFalse(ds.Disabled, "Daemon set disabled field was not set correctly")

	testLabels := klabels.Set{
		pc_fields.AvailabilityZoneLabel: azLabel.String(),
	}
	if matches := ds.NodeSelector.Matches(testLabels); !matches {
		t.Error("The daemon set has a bad node selector")
	}

	originalSHA, err := manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest")
	}
	getSHA, err := ds.Manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest retrieved from daemon set")
	}
	Assert(t).AreEqual(originalSHA, getSHA, "Daemon set manifest not set correctly")

	return ds
}

func TestDelete(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	store := newStore(fixture.Client.KV())
	ds := createDaemonSet(store, fixture.Client.KV(), t)

	if err := store.Delete("bad_id"); err != nil {
		t.Error("Expected no errors while deleting a daemon set that does not exist")
	}

	if err := store.Delete(ds.ID); err != nil {
		t.Errorf("Unable to delete existing daemon set: %s", err)
	}

	if _, _, err := store.Get(ds.ID); err == nil {
		t.Error("Expected to encounter an error while getting a deleted daemon set")
	}

	if err := store.Delete(ds.ID); err != nil {
		t.Error("Expected no errors on while deleting a deleted daemon set")
	}
}

func TestGet(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	store := newStore(fixture.Client.KV())
	//
	// Create DaemonSet
	//
	podID := types.PodID("some_pod_id")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)

	manifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	ds, err := store.Create(ctx, manifest, minHealth, clusterName, selector, podID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	Assert(t).AreNotEqual(ds.ID, "", "Daemon set should have an id")

	//
	// Get DaemonSet and verify it is the same
	//
	getDS, _, err := store.Get(ds.ID)
	if err != nil {
		t.Fatalf("Error retrieving created daemon set: %s", err)
	}

	Assert(t).AreNotEqual(getDS.ID, "", "Daemon set should have an id")
	Assert(t).AreNotEqual(getDS.PodID, "", "Daemon set should have a pod id")

	Assert(t).AreEqual(ds.ID, getDS.ID, "Daemon set should be equal ids")
	Assert(t).AreEqual(ds.PodID, getDS.PodID, "Daemon set should have equal pod ids")
	Assert(t).AreEqual(ds.MinHealth, getDS.MinHealth, "Daemon set should have equal minimum healths")
	Assert(t).AreEqual(ds.Name, getDS.Name, "Daemon set should have equal names")
	Assert(t).AreEqual(ds.Disabled, getDS.Disabled, "Daemon set should have same disabled fields")

	testLabels := klabels.Set{
		pc_fields.AvailabilityZoneLabel: azLabel.String(),
	}
	if matches := getDS.NodeSelector.Matches(testLabels); !matches {
		t.Error("The daemon set has a bad node selector")
	}

	originalSHA, err := manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest")
	}
	getSHA, err := getDS.Manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest retrieved from daemon set")
	}
	Assert(t).AreEqual(originalSHA, getSHA, "Daemon set shas were not equal")

	// Invalid get opertaion
	_, _, err = store.Get("bad_id")
	if err == nil {
		t.Error("Expected get operation to fail when getting a daemon set which does not exist")
	}
}

func TestList(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	store := newStore(fixture.Client.KV())

	// Create first DaemonSet
	firstPodID := types.PodID("some_pod_id")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(firstPodID)

	firstManifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	firstDS, err := store.Create(ctx, firstManifest, minHealth, clusterName, selector, firstPodID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("unable to commit daemon set: %s", err)
	}

	// Create second DaemonSet
	secondPodID := types.PodID("different_pod_id")

	manifestBuilder = manifest.NewBuilder()
	manifestBuilder.SetID(secondPodID)

	secondManifest := manifestBuilder.GetManifest()
	ctx2, cancelFunc2 := transaction.New(context.Background())
	defer cancelFunc2()
	secondDS, err := store.Create(ctx2, secondManifest, minHealth, clusterName, selector, secondPodID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	err = transaction.MustCommit(ctx2, fixture.Client.KV())
	if err != nil {
		t.Fatalf("unable to commit daemon set: %s", err)
	}

	daemonSetList, err := store.List()
	if err != nil {
		t.Fatalf("Error getting list of daemon sets: %s", err)
	}

	Assert(t).AreEqual(len(daemonSetList), 2, "Unexpected number of daemon sets listed")

	for _, daemonSet := range daemonSetList {
		if daemonSet.ID == firstDS.ID {
			Assert(t).AreEqual(daemonSet.PodID, firstPodID, "Listed daemon set pod ids were not equal")

		} else if daemonSet.PodID == secondDS.PodID {
			Assert(t).AreEqual(daemonSet.PodID, secondPodID, "Listed daemon set pod ids were not equal")

		} else {
			t.Errorf("Unexpected daemon set listed: %v", daemonSet)
		}
	}
}

func TestMutate(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	store := newStore(fixture.Client.KV())

	podID := types.PodID("some_pod_id")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	ds, err := store.Create(ctx, podManifest, minHealth, clusterName, selector, podID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("unable to commit daemon set: %s", err)
	}

	//
	// Invalid mutates
	//
	errorMutator := func(dsToMutate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		return dsToMutate, util.Errorf("This is an error")
	}
	_, err = store.MutateDS(ds.ID, errorMutator)
	if err == nil {
		t.Error("Expected error when mutator produces an error")
	}

	badIDMutator := func(dsToMutate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToMutate.ID = ""
		return dsToMutate, nil
	}
	_, err = store.MutateDS(ds.ID, badIDMutator)
	if err == nil {
		t.Error("Expected error when mutating daemon set ID")
	}

	badPodIDMutator := func(dsToMutate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToMutate.PodID = ""
		return dsToMutate, nil
	}
	_, err = store.MutateDS(ds.ID, badPodIDMutator)
	if err == nil {
		t.Error("Expected error when mutating daemon set PodID to mismatch manifest")
	}
	//
	// A valid mutate followed by validation
	//
	someOtherDisabled := !ds.Disabled
	someOtherMinHealth := 42
	someOtherName := ds_fields.ClusterName("some_other_name")
	someOtherPodID := types.PodID("some_other_pod_id")

	manifestBuilder = manifest.NewBuilder()
	manifestBuilder.SetID(someOtherPodID)
	someOtherManifest := manifestBuilder.GetManifest()

	someOtherAZLabel := pc_fields.AvailabilityZone("some_other_zone")
	someOtherSelector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{someOtherAZLabel.String()})

	goodMutator := func(dsToMutate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToMutate.Disabled = someOtherDisabled
		dsToMutate.Manifest = someOtherManifest
		dsToMutate.MinHealth = someOtherMinHealth
		dsToMutate.Name = someOtherName
		dsToMutate.NodeSelector = someOtherSelector
		dsToMutate.PodID = someOtherPodID
		return dsToMutate, nil
	}
	someOtherDS, err := store.MutateDS(ds.ID, goodMutator)
	if err != nil {
		t.Fatalf("Unable to mutate daemon set: %s", err)
	}

	Assert(t).AreEqual(someOtherDS.ID, ds.ID, "Daemon sets should be equal ids")
	Assert(t).AreEqual(someOtherDS.PodID, someOtherPodID, "Daemon sets should have equal pod ids")
	Assert(t).AreEqual(someOtherDS.MinHealth, someOtherMinHealth, "Daemon sets should have equal minimum healths")
	Assert(t).AreEqual(someOtherDS.Name, someOtherName, "Daemon sets should have equal names")
	Assert(t).AreEqual(someOtherDS.Disabled, someOtherDisabled, "Daemon sets should have same disabled fields")

	someOtherLabels := klabels.Set{
		pc_fields.AvailabilityZoneLabel: someOtherAZLabel.String(),
	}
	if matches := someOtherDS.NodeSelector.Matches(someOtherLabels); !matches {
		t.Error("The daemon set has a bad node selector")
	}

	someOtherSHA, err := someOtherManifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest")
	}
	dsSHA, err := someOtherDS.Manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest retrieved from daemon set")
	}
	Assert(t).AreEqual(someOtherSHA, dsSHA, "Daemon set shas were not equal")

	//
	// Validate daemon set from a get function
	//
	getDS, _, err := store.Get(ds.ID)
	if err != nil {
		t.Fatalf("Unable to get daemon set: %s", err)
	}
	Assert(t).AreEqual(getDS.ID, ds.ID, "Daemon sets should be equal ids")
	Assert(t).AreEqual(getDS.PodID, someOtherPodID, "Daemon sets should have equal pod ids")
	Assert(t).AreEqual(getDS.MinHealth, someOtherMinHealth, "Daemon sets should have equal minimum healths")
	Assert(t).AreEqual(getDS.Name, someOtherName, "Daemon sets should have equal names")
	Assert(t).AreEqual(getDS.Disabled, someOtherDisabled, "Daemon sets should have same disabled fields")

	someOtherLabels = klabels.Set{
		pc_fields.AvailabilityZoneLabel: someOtherAZLabel.String(),
	}
	if matches := getDS.NodeSelector.Matches(someOtherLabels); !matches {
		t.Error("The daemon set has a bad node selector")
	}

	someOtherSHA, err = someOtherManifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest")
	}
	dsSHA, err = getDS.Manifest.SHA()
	if err != nil {
		t.Fatal("Unable to retrieve SHA from manifest retrieved from daemon set")
	}
	Assert(t).AreEqual(someOtherSHA, dsSHA, "Daemon set shas were not equal")
}

func TestWatch(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	store := newStore(fixture.Client.KV())
	//
	// Create a new daemon set
	//
	podID := types.PodID("some_pod_id")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	ds, err := store.Create(ctx, podManifest, minHealth, clusterName, selector, podID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("could not commit transaction to create daemon set: %s", err)
	}

	//
	// Create another new daemon set
	//
	someOtherPodID := types.PodID("some_other_pod_id")

	manifestBuilder = manifest.NewBuilder()
	manifestBuilder.SetID(someOtherPodID)
	someOtherManifest := manifestBuilder.GetManifest()

	ctx2, cancelFunc2 := transaction.New(context.Background())
	defer cancelFunc2()
	someOtherDS, err := store.Create(ctx2, someOtherManifest, minHealth, clusterName, selector, someOtherPodID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	err = transaction.MustCommit(ctx2, fixture.Client.KV())
	if err != nil {
		t.Fatalf("could not commit transaction to create daemon set: %s", err)
	}

	//
	// Watch for changes
	//
	quitCh := make(chan struct{})
	inCh := store.Watch(quitCh)
	defer close(quitCh)

	var watched WatchedDaemonSets
	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.Created), 2, "Unexpected number of creates watched")
	Assert(t).AreEqual(len(watched.Updated), 0, "Unexpected number of updates watched")
	Assert(t).AreEqual(len(watched.Deleted), 0, "Unexpected number of deletes watched")

	for _, watchedDS := range watched.Created {
		if watchedDS.ID == ds.ID {
			Assert(t).AreEqual(watchedDS.PodID, ds.PodID, "Daemon sets should have equal pod ids")
		} else if watchedDS.ID == someOtherDS.ID {
			Assert(t).AreEqual(watchedDS.PodID, someOtherDS.PodID, "Daemon sets should have equal pod ids")
		} else {
			t.Errorf("Expected to find id '%s' among watch results, but was not present", watchedDS.ID)
		}
	}

	//
	// Make sure Watch does not output any daemon sets something gets deleted
	//
	err = store.Delete(someOtherDS.ID)
	if err != nil {
		t.Error("Unable to delete daemon set")
	}
	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.Created), 0, "Unexpected number of creates watched")
	Assert(t).AreEqual(len(watched.Updated), 0, "Unexpected number of updates watched")
	Assert(t).AreEqual(len(watched.Deleted), 1, "Unexpected number of deletes watched")

	Assert(t).AreEqual(someOtherDS.ID, watched.Deleted[0].ID, "Daemon sets should have equal ids")

	//
	// Make sure Watch outputs only 1 daemon set when something gets updated
	//
	mutator := func(dsToMutate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToMutate.Disabled = !dsToMutate.Disabled
		return dsToMutate, nil
	}

	ds, err = store.MutateDS(ds.ID, mutator)
	if err != nil {
		t.Fatalf("Unable to mutate daemon set: %s", err)
	}

	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.Created), 0, "Unexpected number of creates watched")
	Assert(t).AreEqual(len(watched.Updated), 1, "Unexpected number of updates watched")
	Assert(t).AreEqual(len(watched.Deleted), 0, "Unexpected number of deletes watched")

	Assert(t).AreEqual(ds.ID, watched.Updated[0].ID, "Daemon sets should have equal ids")
	Assert(t).AreEqual(ds.PodID, watched.Updated[0].PodID, "Daemon sets should have equal pod ids")
}

func TestWatchAll(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	store := newStore(fixture.Client.KV())
	//
	// Create a new daemon set
	//
	podID := types.PodID("some_pod_id")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	ds, err := store.Create(ctx, podManifest, minHealth, clusterName, selector, podID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("could not commit transaction to create daemon set: %s", err)
	}
	//
	// Create another new daemon set
	//
	someOtherPodID := types.PodID("some_other_pod_id")

	manifestBuilder = manifest.NewBuilder()
	manifestBuilder.SetID(someOtherPodID)
	someOtherManifest := manifestBuilder.GetManifest()

	ctx2, cancelFunc2 := transaction.New(context.Background())
	defer cancelFunc2()
	someOtherDS, err := store.Create(ctx2, someOtherManifest, minHealth, clusterName, selector, someOtherPodID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	err = transaction.MustCommit(ctx2, fixture.Client.KV())
	if err != nil {
		t.Fatalf("could not commit transaction to create daemon set: %s", err)
	}
	//
	// Watch for create and verify
	//
	quitCh := make(chan struct{})
	inCh := store.WatchAll(quitCh, 0)
	defer close(quitCh)

	var watched WatchedDaemonSetList
	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.DaemonSets), 2, "Unexpected number of daemon sets watched")

	for _, watchedDS := range watched.DaemonSets {
		if watchedDS.ID == ds.ID {
			Assert(t).AreEqual(watchedDS.PodID, ds.PodID, "Daemon sets should have equal pod ids")
		} else if watchedDS.ID == someOtherDS.ID {
			Assert(t).AreEqual(watchedDS.PodID, someOtherDS.PodID, "Daemon sets should have equal pod ids")
		} else {
			t.Errorf("Expected to find id '%s' among watch results, but was not present", watchedDS.ID)
		}
	}

	//
	// Watch for delete and verify
	//
	err = store.Delete(someOtherDS.ID)
	if err != nil {
		t.Error("Unable to delete daemon set")
	}
	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.DaemonSets), 1, "Unexpected number of daemon sets watched")
	Assert(t).AreEqual(ds.ID, watched.DaemonSets[0].ID, "Daemon sets should have equal ids")

	//
	// Watch for update and verify
	//
	mutator := func(dsToMutate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToMutate.Disabled = !dsToMutate.Disabled
		return dsToMutate, nil
	}

	ds, err = store.MutateDS(ds.ID, mutator)
	if err != nil {
		t.Fatalf("Unable to mutate daemon set: %s", err)
	}

	select {
	case watched = <-inCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Expected something on channel but found nothing")
	}
	if watched.Err != nil {
		t.Errorf("Unexpected error on watched daemon sets: %s", watched.Err)
	}

	Assert(t).AreEqual(len(watched.DaemonSets), 1, "Unexpected number of daemon sets watched")
	Assert(t).AreEqual(ds.ID, watched.DaemonSets[0].ID, "Daemon sets should have equal ids")
	Assert(t).AreEqual(ds.PodID, watched.DaemonSets[0].PodID, "Daemon sets should have equal pod ids")
}

func TestEnableTxnAndDisableTxn(t *testing.T) {
	fixture := consulutil.NewFixture(t)
	defer fixture.Stop()
	store := newStore(fixture.Client.KV())
	//
	// Create a new daemon set
	//
	podID := types.PodID("some_pod_id")
	minHealth := 0
	clusterName := ds_fields.ClusterName("some_name")

	azLabel := pc_fields.AvailabilityZone("some_zone")
	selector := klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.EqualsOperator, []string{azLabel.String()})

	manifestBuilder := manifest.NewBuilder()
	manifestBuilder.SetID(podID)
	podManifest := manifestBuilder.GetManifest()

	timeout := replication.NoTimeout

	ctx, cancelFunc := transaction.New(context.Background())
	defer cancelFunc()
	ds, err := store.Create(ctx, podManifest, minHealth, clusterName, selector, podID, timeout)
	if err != nil {
		t.Fatalf("Unable to create daemon set: %s", err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("could not commit transaction to create daemon set: %s", err)
	}

	ctx, cancelFunc = transaction.New(context.Background())
	defer cancelFunc()
	_, err = store.DisableTxn(ctx, ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("could not commit transaction to disable daemon set: %s", err)
	}

	ds, _, err = store.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if !ds.Disabled {
		t.Fatal("daemon set should have been disabled")
	}

	ctx, cancelFunc = transaction.New(context.Background())
	defer cancelFunc()
	_, err = store.EnableTxn(ctx, ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	err = transaction.MustCommit(ctx, fixture.Client.KV())
	if err != nil {
		t.Fatalf("could not commit transaction to enable daemon set: %s", err)
	}

	ds, _, err = store.Get(ds.ID)
	if err != nil {
		t.Fatal(err)
	}

	if ds.Disabled {
		t.Fatal("daemon set should have been enabled")
	}
}

func newStore(kv consulKV) *ConsulStore {
	return &ConsulStore{
		kv:      kv,
		logger:  logging.DefaultLogger,
		retries: 0,
	}
}
