package stream

import (
	"sync"
)

// StringValuePublisher exposes methods allowing string values to be published via
// channels. The publisher receives a stream of string values from one channel and
// broadcasts the value to all subscribers. When the input channel is closed, all
// subscriptions are closed too. The most recent value is buffered, and when a new
// subscription is created, the recent value is immediately sent (even when the input is
// closed).
type StringValuePublisher struct {
	in <-chan string // the input data stream

	mutex       sync.Mutex            // guards all fields below
	subscribers []*StringSubscription // all subscribers to the input stream
	closed      bool                  // true once the input stream is closed
	value       string                // the last value sent to the input stream
}

// StringSubscription manages one subscription to a StringValuePublisher.
type StringSubscription struct {
	values    chan string           // value channel
	publisher *StringValuePublisher // the publisher subscribed to
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
		for _, s := range p.subscribers {
			// If the subscriber hasn't yet received the last value that was published,
			// remove it and replace it with the current value.
			select {
			case <-s.values:
			default:
			}
			s.values <- val
		}
		p.mutex.Unlock()
	}
	// Send the close to all subscribers
	p.mutex.Lock()
	for _, s := range p.subscribers {
		close(s.values)
	}
	p.in = nil
	p.subscribers = nil
	p.closed = true
	p.mutex.Unlock()
}

// Subscribe creates a new subscription to the publisher's data stream.
func (p *StringValuePublisher) Subscribe() *StringSubscription {
	s := &StringSubscription{
		values:    make(chan string, 1),
		publisher: p,
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	s.values <- p.value
	if p.closed {
		close(s.values)
	} else {
		p.subscribers = append(p.subscribers, s)
	}
	return s
}

func (p *StringValuePublisher) unsubscribe(subscription *StringSubscription) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for i, s := range p.subscribers {
		if s == subscription {
			subLen := len(p.subscribers)
			copy(p.subscribers[i:subLen-1], p.subscribers[i+1:subLen])
			p.subscribers = p.subscribers[:subLen-1]
			return
		}
	}
}

// Chan returns a channel that delivers string values from the publisher. Slow readers may
// miss intermediate updates to the value.
func (s *StringSubscription) Chan() <-chan string {
	return s.values
}

// Unsubscribe cancels the subscription. Sends to the publisher's input channel that
// happen after this method returns will not be delivered to this subscription's channel;
// earlier sends are concurrent and might be delivered.
func (s *StringSubscription) Unsubscribe() {
	s.publisher.unsubscribe(s)
	s.publisher = nil
}
