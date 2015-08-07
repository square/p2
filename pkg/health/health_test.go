package health

import (
	"fmt"
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/kp"
)

func TestPickHealthResult(t *testing.T) {
	catalogResults := []Result{Result{Status: Passing}}
	kvCheck := kp.WatchResult{Status: string(Critical)}
	errCh := make(chan error)
	resultCh := make(chan Result)

	go pickHealthResult([]Result{}, kp.WatchResult{}, fmt.Errorf("no kvCheck"), resultCh, errCh)
	select {
	case _ = <-resultCh:
		t.Fatal("pickhealthresult was passed no results but did not return an error")
	case err := <-errCh:
		Assert(t).AreNotEqual(err, nil, "err should not have been nil")
	}

	go pickHealthResult([]Result{}, kvCheck, nil, resultCh, errCh)
	select {
	case res := <-resultCh:
		Assert(t).AreEqual(res, Result{Status: Critical}, "pick health result did not return the correct kv result")
	case err := <-errCh:
		t.Fatal(fmt.Sprintf("pickhealthresult returned an error: %s, but was passed a valid kvCheck", err))
	}

	go pickHealthResult(catalogResults, kp.WatchResult{}, fmt.Errorf("no kvCheck"), resultCh, errCh)
	select {
	case res := <-resultCh:
		Assert(t).AreEqual(res, Result{Status: Passing}, "pick health result did not return the correct kv result")
	case err := <-errCh:
		t.Fatal(fmt.Sprintf("pickhealthresult returned an error: %s, but was passed a valid catalogue check", err))
	}

	go pickHealthResult(catalogResults, kvCheck, nil, resultCh, errCh)
	select {
	case res := <-resultCh:
		Assert(t).AreEqual(res, Result{Status: Passing}, "pick health result did not return the correct kv result")
	case err := <-errCh:
		t.Fatal(fmt.Sprintf("pickhealthresult returned an error: %s, but was passed a valid checks from both sources", err))
	}
}

func TestFindWorst(t *testing.T) {
	a := Result{
		ID:     "testcrit",
		Status: Critical,
	}
	b := Result{
		ID:     "testwarn",
		Status: Warning,
	}
	c := Result{
		ID:     "testpass",
		Status: Passing,
	}

	id, _, _ := FindWorst([]Result{a, b})
	Assert(t).AreEqual(id, a.ID, "FindWorst between critical and warning should have returned testcrit")

	id, _, _ = FindWorst([]Result{b, c})
	Assert(t).AreEqual(id, b.ID, "FindWorst between warning and passing should have returned testwarn")

	id, _, _ = FindWorst([]Result{c, c})
	Assert(t).AreEqual(id, c.ID, "FindWorst between two passing results should have returned testpass")

	id, _, err := FindWorst([]Result{})
	Assert(t).AreNotEqual(err, nil, "FindWorst did not return error for empty result slice")
}

func TestPickServiceResult(t *testing.T) {
	t1 := mockServiceEntry("1", Passing)
	t2 := mockServiceEntry("2", Passing)
	t3 := mockServiceEntry("3", Passing)

	// Catalog is healthy but KV is not present
	testMap, err := getResult([]*api.ServiceEntry{t1, t2, t3})
	Assert(t).AreEqual(err, nil, "getResult failed")
	catalog := []*api.ServiceEntry{t1, t2, t3}
	res := selectResult(catalog, nil)
	for key, value := range testMap {
		Assert(t).AreEqual(res[key], value, "catalog is healthy, kv not present, selectResult did not match what was expected")
	}

	// Catalog is healthy but KV is not
	watchRes := mockWatchResult(catalog, Critical)
	res = selectResult(catalog, watchRes)
	for key, value := range testMap {
		Assert(t).AreEqual(res[key], value, "catalog is healthy, kv is not, selectResult did not match what was expected")
	}

	// KV is healthy but catalog is not present
	kv := mockWatchResult(catalog, Passing)
	res = selectResult(nil, kv)
	for key, value := range testMap {
		Assert(t).AreEqual(consulWatchToResult(kv[key]), value, "kv is healthy, catalog not present, selectResult did not match what was expected")
	}

	t1 = mockServiceEntry("1", Critical)
	t2 = mockServiceEntry("2", Critical)
	t3 = mockServiceEntry("3", Critical)
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
	passingCheck := []*api.HealthCheck{fakeAPICheck("s", Passing)}
	passingChecker := ConsulHealthChecker{health: fakeNodeChecker{
		passingCheck,
		&api.QueryMeta{LastIndex: 1337},
		nil,
	},
	}

	_, index, err := passingChecker.fetchNodeHealth("", 0)
	Assert(t).AreEqual(uint64(1337), index, "Should have received the new index")
	Assert(t).IsNil(err, "Should not have received an error")

	erringChecker := ConsulHealthChecker{health: fakeNodeChecker{
		nil,
		nil,
		fmt.Errorf("It messed up!"),
	},
	}

	_, index, err = erringChecker.fetchNodeHealth("", 1337)
	Assert(t).IsNotNil(err, "Should have been an error from the checker")
	Assert(t).AreEqual(uint64(1337), index, "Should have assigned the index to the old index value due to error")
}

func mockWatchResult(entries []*api.ServiceEntry, st HealthState) map[string]kp.WatchResult {
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

func getResult(entries []*api.ServiceEntry) (map[string]Result, error) {
	var HEErr *HealthEmpty
	res := make(map[string]Result)
	for _, entry := range entries {
		val := make([]Result, 0, len(entry.Checks))
		for _, check := range entry.Checks {
			val = append(val, consulCheckToResult(*check))
		}
		res[entry.Node.Node], HEErr = findWorstResult(val)
		if HEErr != nil {
			return res, HEErr
		}
	}

	return res, nil
}

func mockServiceEntry(s string, st HealthState) *api.ServiceEntry {
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

func fakeAPICheck(s string, st HealthState) *api.HealthCheck {
	return &api.HealthCheck{
		CheckID:   "test" + s,
		Node:      "node" + s,
		ServiceID: "service" + s,
		Status:    string(st),
	}
}
