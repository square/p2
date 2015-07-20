package health

import (
	"testing"

	. "github.com/anthonybishopric/gotcha"
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
	Assert(t).AreEqual(id, a.ID, "FindWorst should have returned testcrit")

	id, _ = FindWorst([]Result{b, c})
	Assert(t).AreEqual(id, b.ID, "FindWorst should have returned testwarn")

	id, _ = FindWorst([]Result{c, c})
	Assert(t).AreEqual(id, c.ID, "FindWorst should have returned testpass")
}
