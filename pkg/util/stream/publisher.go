package stream

import (
	"sync"
)

// StringValuePublisher exposes methods allowing string values to be published via
// channels. The publisher receives a stream of string values from one channel and
// broadcasts (via a blocking channel send) the value to all subscribers. When the input
// channel is closed, all subscriptions are closed too. The most recent value is buffered,
// and when a new subscription is created, the recent value is immediately sent (even when
// the input is closed).
type StringValuePublisher struct {
	in <-chan string // the input data stream

	mutex       sync.Mutex            // guards all fields below
	subscribers []*StringSubscription // all subscribers to the input stream
	closed      bool                  // true once the input stream is closed
	value       string                // the last value sent to the input stream
}

// StringSubscription manages one subscription to a StringValuePublisher.
type StringSubscription struct {
	values      chan string           // value channel
	initialized chan struct{}         // closed after the first value has been sent
	canceled    chan struct{}         // closed when unsubscribed
	publisher   *StringValuePublisher // the publisher subscribed to
}

// NewStringPublisher creates a new StringPublisher that reads from the given input
// channel. Subscribers will receive initValue if they subscribe before the first value is
// received from the input channel.
func NewStringValuePublisher(in <-chan string, initValue string) *StringValuePublisher {
	p := &StringValuePublisher{in: in, value: initValue}
	go p.read()
	return p
}

func (p *StringValuePublisher) read() {
	// Send values to all subscribers
	for val := range p.in {
		p.mutex.Lock()
		p.value = val
		subs := p.subscribers
		p.mutex.Unlock()
		for _, s := range subs {
			if ok := s.waitForReady(); ok {
				select {
				case s.values <- val:
					// This send might block if the subscriber has quit receiving from its
					// values channel.
				case <-s.canceled:
				}
			}
		}
	}
	// Send the close to all subscribers
	p.mutex.Lock()
	subs := p.subscribers
	p.in = nil
	p.subscribers = nil
	p.closed = true
	p.mutex.Unlock()
	for _, s := range subs {
		if ok := s.waitForReady(); ok {
			s.close()
		}
	}
}

// Subscribe creates a new subscription to the publisher's data stream.
func (p *StringValuePublisher) Subscribe() *StringSubscription {
	s := &StringSubscription{
		values:      make(chan string),
		initialized: make(chan struct{}),
		canceled:    make(chan struct{}),
		publisher:   p,
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	// Send the first value in another goroutine to avoid blocking this one
	if p.closed {
		go func(firstValue string) {
			s.sendFirst(firstValue)
			s.close()
		}(p.value)
	} else {
		// Append doesn't modify existing slices, so it's still safe.
		p.subscribers = append(p.subscribers, s)
		go s.sendFirst(p.value)
	}
	return s
}

func (p *StringValuePublisher) unsubscribe(subscription *StringSubscription) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for i, s := range p.subscribers {
		if s == subscription {
			// Subscribe() and read() assume their slices' contents are immutable
			subLen := len(p.subscribers)
			newSub := make([]*StringSubscription, subLen-1)
			copy(newSub[:i], p.subscribers[:i])
			copy(newSub[i:subLen-1], p.subscribers[i+1:subLen])
			p.subscribers = newSub
			return
		}
	}
}

// Chan returns a channel that delivers string values from the publisher. Returns a
// receive-only channel with a buffer size of 1.
func (s *StringSubscription) Chan() <-chan string {
	return s.values
}

// Unsubscribe cancels the subscription. Sends to the publisher's input channel that
// happen after this method returns will not be delivered to this subscription's channel;
// earlier sends are concurrent and might be delivered.
func (s *StringSubscription) Unsubscribe() {
	s.publisher.unsubscribe(s)
	s.publisher = nil
	close(s.canceled)
}

// waitForReady blocks until the sendFirst() goroutine finishes executing. At that point,
// either (1) the subscription has received its first value and is ready to receive
// further streaming values, or (2) the subscription has been canceled and there should be
// no further attempts to send values.
//
// Returns false if the subscription has been canceled. Returns true if the subscriber
// hasn't yet canceled.
func (s *StringSubscription) waitForReady() bool {
	<-s.initialized
	select {
	case <-s.canceled:
		return false
	default:
		return true
	}
}

// sendFirst sends the first value to the subscription stream, then marks the subscription
// as ready for more streaming data.
func (s *StringSubscription) sendFirst(val string) {
	defer close(s.initialized)
	select {
	case s.values <- val:
	case <-s.canceled:
	}
}

// close the subscription's data stream if the subscription wasn't previously canceled
func (s *StringSubscription) close() {
	select {
	default:
		close(s.values)
	case <-s.canceled:
	}
}
