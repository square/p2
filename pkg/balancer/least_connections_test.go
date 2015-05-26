package balancer

import (
	"testing"
	"time"

	. "github.com/anthonybishopric/gotcha"
)

func TestLeastConnectionsRoutableWithEnabledHosts(t *testing.T) {
	l, err := NewLeastConnectionsStrategy([]string{"127.0.0.1:8080"})
	Assert(t).IsNil(err, "Should not have erred setting up the strategy")

	Assert(t).IsNil(l.Routable(time.Millisecond), "Should have been routable")
}

func TestLeastConnectionsNotRoutableWithDisabledHosts(t *testing.T) {
	l, err := NewLeastConnectionsStrategy([]string{"127.0.0.1:8080"})
	Assert(t).IsNil(err, "Should not have erred setting up the strategy")

	Assert(t).IsNil(l.RemoveAddress("127.0.0.1:8080"), "should not have failed to disable the address")

	Assert(t).IsNotNil(l.Routable(time.Millisecond), "should not have been routable with every host disabled")

	_, err = l.acquireAddress()
	Assert(t).IsNotNil(err, "should have failed to acquire a new address")
}

func TestLeastConnectionsPicksHostWithFewestConnections(t *testing.T) {
	l, err := NewLeastConnectionsStrategy([]string{"127.0.0.1:8080", "127.0.0.1:7899", "127.0.0.1:4455"})
	Assert(t).IsNil(err, "Should not have erred setting up the strategy")

	// Acquire two connections, verify that the third doesn't match the first two
	first, err := l.acquireAddress()
	Assert(t).IsNil(err, "should have acquired a first connection")
	second, err := l.acquireAddress()
	Assert(t).IsNil(err, "should have acquired a second connection")

	Assert(t).AreNotEqual(first, second, "The addresses selected should have been different")

	third, err := l.acquireAddress()
	Assert(t).IsNil(err, "should have acquired a third connection")

	Assert(t).AreNotEqual(first, third, "The third address selected should have been different from the first")
	Assert(t).AreNotEqual(second, third, "The third address selected should have been different from the second")

	// Release the second connection, re-acquire and show that it's the same host
	Assert(t).IsNil(l.releaseAddress(second), "should have been able to release the second address")

	fourth, err := l.acquireAddress()
	Assert(t).IsNil(err, "should not have failed to acquire a fourth connection")

	Assert(t).AreEqual(second, fourth, "The second host should have been returned as the fourth since it was released.")
}
