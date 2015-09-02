package health

import (
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
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

	id, _, _ := FindWorst([]Result{a, b})
	Assert(t).AreEqual(id, a.ID, "FindWorst between critical and warning should have returned testcrit")

	id, _, _ = FindWorst([]Result{b, c})
	Assert(t).AreEqual(id, b.ID, "FindWorst between warning and passing should have returned testwarn")

	id, _, _ = FindWorst([]Result{c, c})
	Assert(t).AreEqual(id, c.ID, "FindWorst between two passing results should have returned testpass")

	id, _, err := FindWorst([]Result{})
	Assert(t).AreNotEqual(err, nil, "FindWorst did not return error for empty result slice")
}
