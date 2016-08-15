package stream

import (
	"reflect"
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

// Test that sending to a publisher does not block when there are no subscribers
func TestSink(t *testing.T) {
	t.Parallel()
	input := make(chan string)
	_ = NewStringValuePublisher(input, "")
	input <- "this"
	input <- "doesn't"
	input <- "block"
}

// Test that a subscription gets the latest value that happened before Subscribe().
func TestLastValue(t *testing.T) {
	t.Parallel()
	input := make(chan string)
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
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		if v := <-s1.Chan(); v != "world" {
			t.Error("did not receive second value. got:", v)
		}
		wg.Done()
	}()
	go func() {
		if v := <-s2.Chan(); v != "world" {
			t.Error("did not receive second value. got:", v)
		}
		wg.Done()
	}()
	wg.Wait()
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
	if !reflect.DeepEqual(vals, seq) {
		t.Error("channel didn't receive all values. got:", vals)
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
	p := NewStringValuePublisher(input, "init")

	s1 := p.Subscribe() // sends "init"
	unsubscribed := make(chan struct{})
	sentMore := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		if val := <-s1.Chan(); val != "init" {
			t.Error("dropped first value, got", val)
		}
		if val := <-s1.Chan(); val != "hello" {
			t.Error("dropped second value, got", val)
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
	p := NewStringValuePublisher(input, "init")
	s := p.Subscribe()
	s.Unsubscribe()
	s = nil

	input <- "no"
	input <- "blocking"
	input <- "here"
}

// A complex test of unsubscriptions. Doesn't test anything in particular.
func TestUnsubscribeComplex(t *testing.T) {
	t.Parallel()
	input := make(chan string, 3)
	p := NewStringValuePublisher(input, "init")

	s1 := p.Subscribe()
	<-s1.Chan()
	s2 := p.Subscribe()
	<-s2.Chan()

	// Queue messages to be sent to the subscribers
	input <- "A"
	input <- "B"
	input <- "C"
	close(input)

	// S2 will receive "A" then unsubscribe, maybe receive "B", never receive "C"
	unsubscribed := make(chan struct{})
	doneSending := make(chan struct{})
	doneChecking := make(chan struct{})
	go func() {
		v1, ok := <-s2.Chan()
		if v1 != "A" {
			t.Error("did not receive expected value 'A'. got:", v1)
		}
		if !ok {
			t.Error("subscription channel closed early")
		}

		s2.Unsubscribe()
		close(unsubscribed)

		values, ok := receiveAll(s2.Chan(), doneSending)
		if !ok {
			t.Error("s2 should not be closed")
		}
		if len(values) > 0 && values[0] != "B" {
			t.Error("expected to receive 'B', got:", values[0])
		}
		if len(values) > 1 {
			t.Error("received too many values:", values)
		}
		close(doneChecking)
	}()
	<-s1.Chan()
	<-unsubscribed
	<-s1.Chan()
	<-s1.Chan()
	close(doneSending)
	<-doneChecking
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
	s2.Unsubscribe()
	input <- "bar"
	input <- "blah"
	close(input)

	v1 := <-v1Chan
	v3 := <-v3Chan
	close(doneSending)
	v2 := <-v2Chan
	expected := []string{"init", "foo", "bar", "blah"}
	if !reflect.DeepEqual(v1, expected) {
		t.Error("sub1 got unexpected sequence:", v1)
	}
	// Receives are asynchronous. The only error is receiving a value whose send happened
	// after the unsubscribe.
	if len(v2) > 2 || !reflect.DeepEqual(v2, expected[:len(v2)]) {
		t.Error("sub2 got unexpected sequence:", v2)
	}
	if !reflect.DeepEqual(v3, expected) {
		t.Error("sub3 got unexpected sequence:", v3)
	}
}
