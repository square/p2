package health

import (
	"testing"

	. "github.com/anthonybishopric/gotcha"
	"github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/kp"
)

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

	id, _ := FindWorst([]Result{a, b})
	Assert(t).AreEqual(id, a.ID, "FindWorst between critical and warning should have returned testcrit")

	id, _ = FindWorst([]Result{b, c})
	Assert(t).AreEqual(id, b.ID, "FindWorst between warning and passing should have returned testwarn")

	id, _ = FindWorst([]Result{c, c})
	Assert(t).AreEqual(id, c.ID, "FindWorst between two passing results should have returned testpass")
}

func TestPickServiceResult(t *testing.T) {
	t1 := mockServiceEntry("1", "passing")
	t2 := mockServiceEntry("2", "passing")
	t3 := mockServiceEntry("3", "passing")

	// Catalog is healthy but KV is not present
	testMap := getResult([]*api.ServiceEntry{t1, t2, t3})
	catalog := []*api.ServiceEntry{t1, t2, t3}
	res, err := selectResult(catalog, nil)
	Assert(t).AreEqual(err, nil, "catalog is healthy, kv not present and selectResult returned err")
	for key, value := range testMap {
		Assert(t).AreEqual(res[key], value, "catalog is healthy, kv not present, selectResult did not match what was expected")
	}

	// Catalog is healthy but KV is not
	watchRes := mockWatchResult(catalog, Critical)
	res, err = selectResult(catalog, watchRes)
	Assert(t).AreEqual(err, nil, "catalog is healthy but kv is not and selectResult returned err")
	for key, value := range testMap {
		Assert(t).AreEqual(res[key], value, "catalog is healthy, kv is not, selectResult did not match what was expected")
	}

	// KV is healthy but catalog is not present
	kv := mockWatchResult(catalog, Passing)
	res, err = selectResult(nil, kv)
	Assert(t).AreEqual(err, nil, "kv is healthy, catalog not present and selectResult returned err")
	for key, value := range testMap {
		Assert(t).AreEqual(consulWatchToResult(kv[key]), value, "kv is healthy, catalog not present, selectResult did not match what was expected")
	}

	t1 = mockServiceEntry("1", "critical")
	t2 = mockServiceEntry("2", "critical")
	t3 = mockServiceEntry("3", "critical")
	catalog = []*api.ServiceEntry{t1, t2, t3}

	// KV is healthy but catalog is not
	res, err = selectResult(catalog, kv)
	Assert(t).AreEqual(err, nil, "kv is healthy, catalog is not and selectResult returned err")
	for key, value := range testMap {
		Assert(t).AreEqual(consulWatchToResult(kv[key]), value, "kv is healthy, catalog is not and selectResult did not match what was expected")
	}
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

func getResult(entries []*api.ServiceEntry) map[string]Result {
	res := make(map[string]Result)
	for _, entry := range entries {
		val := make([]Result, 0, len(entry.Checks))
		for _, check := range entry.Checks {
			val = append(val, consulCheckToResult(*check))
		}
		res[entry.Node.Node] = findWorstResult(val)
	}

	return res
}

func mockServiceEntry(s, st string) *api.ServiceEntry {
	ret := &api.ServiceEntry{
		Node: newAPINode(s),
	}
	h1 := newAPICheck(s+"1", st)
	h2 := newAPICheck(s+"2", st)
	h3 := newAPICheck(s+"3", st)

	ret.Checks = []*api.HealthCheck{h1, h2, h3}
	return ret
}

func newAPINode(s string) *api.Node {
	return &api.Node{
		Node: "node" + s,
	}
}

func newAPICheck(s, st string) *api.HealthCheck {
	return &api.HealthCheck{
		CheckID:   "test" + s,
		Node:      "node" + s,
		ServiceID: "service" + s,
		Status:    st,
	}
}
