package checker

import (
	"fmt"
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/kp"
)

func TestPickHealthResult(t *testing.T) {
	catalogResults := health.ResultList{health.Result{Status: health.Passing}}
	kvCheck := kp.WatchResult{Status: string(health.Critical)}
	errCh := make(chan error)
	resultCh := make(chan health.Result)

	go pickHealthResult(health.ResultList{}, kp.WatchResult{}, fmt.Errorf("no kvCheck"), resultCh, errCh)
	select {
	case _ = <-resultCh:
		t.Fatal("pickhealthresult was passed no results but did not return an error")
	case err := <-errCh:
		Assert(t).AreNotEqual(err, nil, "err should not have been nil")
	}

	go pickHealthResult(health.ResultList{}, kvCheck, nil, resultCh, errCh)
	select {
	case res := <-resultCh:
		Assert(t).AreEqual(res, health.Result{Status: health.Critical}, "pick health result did not return the correct kv result")
	case err := <-errCh:
		t.Fatal(fmt.Sprintf("pickhealthresult returned an error: %s, but was passed a valid kvCheck", err))
	}

	go pickHealthResult(catalogResults, kp.WatchResult{}, fmt.Errorf("no kvCheck"), resultCh, errCh)
	select {
	case res := <-resultCh:
		Assert(t).AreEqual(res, health.Result{Status: health.Passing}, "pick health result did not return the correct kv result")
	case err := <-errCh:
		t.Fatal(fmt.Sprintf("pickhealthresult returned an error: %s, but was passed a valid catalogue check", err))
	}

	go pickHealthResult(catalogResults, kvCheck, nil, resultCh, errCh)
	select {
	case res := <-resultCh:
		Assert(t).AreEqual(res, health.Result{Status: health.Passing}, "pick health result did not return the correct kv result")
	case err := <-errCh:
		t.Fatal(fmt.Sprintf("pickhealthresult returned an error: %s, but was passed a valid checks from both sources", err))
	}
}

func TestPickServiceResult(t *testing.T) {
	t1 := mockServiceEntry("1", health.Passing)
	t2 := mockServiceEntry("2", health.Passing)
	t3 := mockServiceEntry("3", health.Passing)

	// Catalog is healthy but KV is not present
	testMap, err := getResult([]*api.ServiceEntry{t1, t2, t3})
	Assert(t).AreEqual(err, nil, "getResult failed")
	catalog := []*api.ServiceEntry{t1, t2, t3}
	res := selectResult(catalog, nil)
	for key, value := range testMap {
		Assert(t).AreEqual(res[key], value, "catalog is healthy, kv not present, selectResult did not match what was expected")
	}

	// Catalog is healthy but KV is not
	watchRes := mockWatchResult(catalog, health.Critical)
	res = selectResult(catalog, watchRes)
	for key, value := range testMap {
		Assert(t).AreEqual(res[key], value, "catalog is healthy, kv is not, selectResult did not match what was expected")
	}

	// KV is healthy but catalog is not present
	kv := mockWatchResult(catalog, health.Passing)
	res = selectResult(nil, kv)
	for key, value := range testMap {
		Assert(t).AreEqual(consulWatchToResult(kv[key]), value, "kv is healthy, catalog not present, selectResult did not match what was expected")
	}

	t1 = mockServiceEntry("1", health.Critical)
	t2 = mockServiceEntry("2", health.Critical)
	t3 = mockServiceEntry("3", health.Critical)
	catalog = []*api.ServiceEntry{t1, t2, t3}

	// KV is healthy but catalog is not
	res = selectResult(catalog, kv)
	for key, value := range testMap {
		Assert(t).AreEqual(consulWatchToResult(kv[key]), value, "kv is healthy, catalog is not and selectResult did not match what was expected")
	}
}

type fakeNodeChecker struct {
	checks []*api.HealthCheck
	meta   *api.QueryMeta
	err    error
}

func (f fakeNodeChecker) Node(_ string, _ *api.QueryOptions) ([]*api.HealthCheck, *api.QueryMeta, error) {
	return f.checks, f.meta, f.err
}

func TestFetchHealth(t *testing.T) {
	passingCheck := []*api.HealthCheck{fakeAPICheck("s", health.Passing)}
	passingChecker := consulHealthChecker{health: fakeNodeChecker{
		passingCheck,
		&api.QueryMeta{LastIndex: 1337},
		nil,
	},
	}

	_, index, err := passingChecker.fetchNodeHealth("", 0)
	Assert(t).AreEqual(uint64(1337), index, "Should have received the new index")
	Assert(t).IsNil(err, "Should not have received an error")

	erringChecker := consulHealthChecker{health: fakeNodeChecker{
		nil,
		nil,
		fmt.Errorf("It messed up!"),
	},
	}

	_, index, err = erringChecker.fetchNodeHealth("", 1337)
	Assert(t).IsNotNil(err, "Should have been an error from the checker")
	Assert(t).AreEqual(uint64(1337), index, "Should have assigned the index to the old index value due to error")
}

func mockWatchResult(entries []*api.ServiceEntry, st health.HealthState) map[string]kp.WatchResult {
	ret := make(map[string]kp.WatchResult)
	for _, entry := range entries {
		newWatch := kp.WatchResult{
			Id:      entry.Checks[0].CheckID,
			Node:    entry.Checks[0].Node,
			Service: entry.Checks[0].ServiceID,
			Status:  string(st),
		}
		ret[entry.Node.Node] = newWatch
	}
	return ret
}

func getResult(entries []*api.ServiceEntry) (map[string]health.Result, error) {
	res := make(map[string]health.Result)
	for _, entry := range entries {
		val := make(health.ResultList, 0, len(entry.Checks))
		for _, check := range entry.Checks {
			val = append(val, consulCheckToResult(*check))
		}
		r := val.MinValue()
		if r == nil {
			res[entry.Node.Node] = health.Result{Status: health.Critical}
			return res, fmt.Errorf("no results were passed to findWorstResult")
		}
		res[entry.Node.Node] = *r
	}

	return res, nil
}

func mockServiceEntry(s string, st health.HealthState) *api.ServiceEntry {
	ret := &api.ServiceEntry{
		Node: newAPINode(s),
	}
	h1 := fakeAPICheck(s+"1", st)
	h2 := fakeAPICheck(s+"2", st)
	h3 := fakeAPICheck(s+"3", st)

	ret.Checks = []*api.HealthCheck{h1, h2, h3}
	return ret
}

func newAPINode(s string) *api.Node {
	return &api.Node{
		Node: "node" + s,
	}
}

func fakeAPICheck(s string, st health.HealthState) *api.HealthCheck {
	return &api.HealthCheck{
		CheckID:   "test" + s,
		Node:      "node" + s,
		ServiceID: "service" + s,
		Status:    string(st),
	}
}
