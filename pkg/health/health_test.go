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

	m := MinResult(a, b)
	Assert(t).AreEqual(m.ID, a.ID, "MinValue between critical and warning should have returned testcrit")

	m = MinResult(c, b)
	Assert(t).AreEqual(m.ID, b.ID, "MinValue between warning and passing should have returned testwarn")

	m = MinResult(c, c)
	Assert(t).AreEqual(m.ID, c.ID, "MinValue between two passing results should have returned testpass")

	mp := ResultList{}.MinValue()
	Assert(t).AreEqual(mp, (*Result)(nil), "MinValue found a min value for empty result slice")
}
