package logging

import (
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
)

func TestProcessCounterIncrementsEveryTime(t *testing.T) {
	counter := processCounter.counter
	Assert(t).AreEqual(counter, processCounter.Fields()["Counter"], "The counter was wrong")
	Assert(t).AreEqual(counter+1, processCounter.Fields()["Counter"], "The counter was wrong")
	Assert(t).AreEqual(counter+2, processCounter.Fields()["Counter"], "The counter was wrong")
}
