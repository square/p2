package stream

import (
	"sync"
	"testing"
	"time"
)

// Consume all messages until the channel would block. Returns the number of messages
// received.
func flush(c <-chan string) int {
	count := 0
	for {
		select {
		case _, ok := <-c:
			if !ok {
				return count
			}
			count++
		default:
			return count
		}
	}
}

// Consume all messages on a channel until canceled. Returns
// the values received and whether the channel is still open.
func receiveAll(c <-chan string, done <-chan struct{}) (values []string, ok bool) {
	var val string
	values = make([]string, 0)
	ok = true
	for {
		select {
		case val, ok = <-c:
			if !ok {
				return
			}
			values = append(values, val)
		case <-done:
			return
		}
	}
}

func receiveAllAsync(c <-chan string, done <-chan struct{}) <-chan []string {
	retChan := make(chan []string)
	go func() {
		ret, _ := receiveAll(c, done)
		retChan <- ret
	}()
	return retChan
}

// validRecv checks if a sequence of values received by a subscriber is consistent with
// the values sent to the publisher. The two are not necessarily the same because the
// receiver might omit intermediate values, though the final value should be the same.
func validRecv(sent, received []string) bool {
RecvLoop:
	for _, r := range received {
		for i, s := range sent {
			if r == s {
				sent = sent[i+1 : len(sent)]
				continue RecvLoop
			}
		}
		return false
	}
	return len(sent) == 0
}

// Test that sending to a publisher does not block when there are no subscribers
func TestSink(t *testing.T) {
	t.Parallel()
	input := make(chan string)
	defer close(input)
	_ = NewStringValuePublisher(input, "")
	input <- "this"
	input <- "doesn't"
	input <- "block"
}

// Test that a subscription gets the latest value that happened before Subscribe().
func TestLastValue(t *testing.T) {
	t.Parallel()
	input := make(chan string)
	defer close(input)
	p := NewStringValuePublisher(input, "init")

	s1 := p.Subscribe()
	if v := <-s1.Chan(); v != "init" {
		t.Error("did not receive initial value. got:", v)
	}

	input <- "hello"
	if v := <-s1.Chan(); v != "hello" {
		t.Error("did not receive first value. got:", v)
	}
	s2 := p.Subscribe()
	if v := <-s2.Chan(); v != "hello" {
		t.Error("subscription did not get most recent value. got:", v)
	}

	input <- "world"
	if v := <-s1.Chan(); v != "world" {
		t.Error("did not receive second value. got:", v)
	}
	if v := <-s2.Chan(); v != "world" {
		t.Error("did not receive second value. got:", v)
	}
}

// Test that when the input channel is closed, subscription channels are closed too.
func TestClose(t *testing.T) {
	t.Parallel()
	seq := []string{"X", "Y", "Z", "W"}

	input := make(chan string)
	p := NewStringValuePublisher(input, seq[0])
	s := p.Subscribe()
	// Read messages until the channel is closed
	vChan := receiveAllAsync(s.Chan(), nil)

	for _, v := range seq[1:] {
		input <- v
	}
	close(input)

	vals := <-vChan
	if !validRecv(seq, vals) {
		t.Error("inconsistent: sent", vals, "received", vals)
	}
}

// Test that subscribing to a closed publisher still gets the latest value, then closes.
func TestAlwaysClosed(t *testing.T) {
	t.Parallel()
	input := make(chan string)
	close(input)
	p := NewStringValuePublisher(input, "first")
	s := p.Subscribe()
	if v, ok := <-s.Chan(); !ok || v != "first" {
		t.Errorf("received wrong value: %v/%v", v, ok)
	}
	if _, ok := <-s.Chan(); ok {
		t.Errorf("channel did not close properly")
	}
}

// Test that unsubscribing basically works.
func TestUnsubscribe(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup
	defer wg.Wait()
	input := make(chan string)
	defer close(input)
	p := NewStringValuePublisher(input, "init")

	s1 := p.Subscribe() // sends "init"
	unsubscribed := make(chan struct{})
	sentMore := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		v := <-s1.Chan()
		if v == "init" {
			v = <-s1.Chan()
		}
		if v != "hello" {
			t.Fatal("received unexpected value", v)
		}
		s1.Unsubscribe()
		close(unsubscribed)
		<-sentMore

		// No more messages should be received, but we can't wait forever to verify that.
		select {
		case val := <-s1.Chan():
			t.Error("got extra value", val)
		case <-time.After(20 * time.Millisecond):
		}
	}()

	input <- "hello"
	<-unsubscribed
	// These sends happen after the unsubscribe
	input <- "world"
	input <- "this shouldn't block"
	close(sentMore)
}

// Test that immediately unsubscribing after subscribing won't block a sender.
func TestUnsubscribeImmediate(t *testing.T) {
	t.Parallel()
	input := make(chan string, 1)
	defer close(input)
	p := NewStringValuePublisher(input, "init")
	s := p.Subscribe()
	s.Unsubscribe()
	s = nil

	input <- "no"
	input <- "blocking"
	input <- "here"
}

// Test that unsubscribing an arbitrary subscriber doesn't block the other subscribers.
func TestUnsubscribeIndependence(t *testing.T) {
	t.Parallel()
	input := make(chan string)
	p := NewStringValuePublisher(input, "init")

	doneSending := make(chan struct{})
	s1 := p.Subscribe()
	v1Chan := receiveAllAsync(s1.Chan(), nil)
	s2 := p.Subscribe()
	v2Chan := receiveAllAsync(s2.Chan(), doneSending)
	s3 := p.Subscribe()
	v3Chan := receiveAllAsync(s3.Chan(), nil)

	input <- "foo"
	s2.Unsubscribe() // concurrent with sending "foo" to s2
	input <- "bar"
	input <- "blah"
	close(input)

	v1 := <-v1Chan
	v3 := <-v3Chan
	close(doneSending)
	v2 := <-v2Chan
	expected := []string{"init", "foo", "bar", "blah"}
	if !validRecv(expected, v1) {
		t.Error("sub1 got unexpected sequence:", v1)
	}
	// Receives are asynchronous. The only error is receiving a value whose send happened
	// after the unsubscribe.
	for _, s := range v2 {
		if s == "bar" || s == "blah" {
			t.Error("sub2 got unexpected sequence:", v2)
		}
	}
	if !validRecv(expected, v3) {
		t.Error("sub3 got unexpected sequence:", v3)
	}
}
