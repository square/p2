package consulutil

import (
	"bytes"
	"time"

	"github.com/hashicorp/consul/api"
)

// WatchPrefix watches a Consul prefix for changes to any keys that have the prefix. When
// anything changes, all Key/Value pairs having that prefix will be written to the
// provided channel.
//
// Errors will sent on the given output channel but do not otherwise affect execution. The
// given output stream will become owned by this function call, and this call will close
// it when the function ends. This function will run until explicitly canceled by closing
// the "done" channel. Data is written to the output channel synchronously, so readers
// must consume the data or this method will block.
func WatchPrefix(
	prefix string,
	clientKV ConsulLister,
	outPairs chan<- api.KVPairs,
	done <-chan struct{},
	outErrors chan<- error,
) {
	defer close(outPairs)
	var currentIndex uint64
	timer := time.NewTimer(time.Duration(0))

	for {
		select {
		case <-done:
			return
		case <-timer.C:
		}
		timer.Reset(250 * time.Millisecond) // upper bound on request rate
		pairs, queryMeta, err := SafeList(clientKV, done, prefix, &api.QueryOptions{
			WaitIndex: currentIndex,
		})
		switch err {
		case CanceledError:
			return
		case nil:
			currentIndex = queryMeta.LastIndex
			select {
			case <-done:
			case outPairs <- pairs:
			}
		default:
			select {
			case <-done:
			case outErrors <- err:
			}
			timer.Reset(2 * time.Second) // backoff
		}
	}
}

// WatchSingle has the same semantics as WatchPrefix, but for a single key in
// Consul. If the key is deleted, a nil will be sent on the output channel, but
// the watch will not be terminated. In addition, if updates happen in rapid
// succession, intervening updates may be missed. If these semantics are
// undesirable, consider WatchNewKeys instead.
func WatchSingle(
	key string,
	clientKV ConsulGetter,
	outKVP chan<- *api.KVPair,
	done <-chan struct{},
	outErrors chan<- error,
) {
	defer close(outKVP)
	var currentIndex uint64
	timer := time.NewTimer(time.Duration(0))

	for {
		select {
		case <-done:
			return
		case <-timer.C:
		}
		timer.Reset(250 * time.Millisecond) // upper bound on request rate
		kvp, queryMeta, err := SafeGet(clientKV, done, key, &api.QueryOptions{
			WaitIndex: currentIndex,
		})
		switch err {
		case CanceledError:
			return
		case nil:
			currentIndex = queryMeta.LastIndex
			select {
			case <-done:
			case outKVP <- kvp:
			}
		default:
			select {
			case <-done:
			case outErrors <- err:
			}
			timer.Reset(2 * time.Second) // backoff
		}
	}
}

type NewKeyHandler func(key string) chan<- *api.KVPair

type keyMeta struct {
	created    uint64
	modified   uint64
	subscriber chan<- *api.KVPair
}

// WatchNewKeys watches for changes to a list of Key/Value pairs and lets each key be
// handled individually though a subscription-like interface.
//
// This function models a key's lifetime in the following way. When a key is first seen,
// the given NewKeyHandler function will be run, which may return a channel. When the
// key's value changes, new K/V updates are sent to the key's notification channel. When
// the key is deleted, `nil` is sent. After being deleted or if the watcher is asked to
// exit, a key's channel will be closed, to notify the receiver that no further updates
// are coming.
//
// WatchNewKeys doesn't watch a prefix itself--the caller should arrange a suitable input
// stream of K/V pairs, probably from WatchPrefix(). This function runs until the input
// stream closes. Closing "done" will asynchronously cancel the watch and cause it to
// eventually exit.
func WatchNewKeys(pairsChan <-chan api.KVPairs, onNewKey NewKeyHandler, done <-chan struct{}) {
	keys := make(map[string]*keyMeta)

	defer func() {
		for _, keyMeta := range keys {
			if keyMeta.subscriber != nil {
				close(keyMeta.subscriber)
			}
		}
	}()

	for {
		var pairs api.KVPairs
		var ok bool
		select {
		case <-done:
			return
		case pairs, ok = <-pairsChan:
			if !ok {
				return
			}
		}

		visited := make(map[string]bool)

		// Scan for new and changed keys
		for _, pair := range pairs {
			visited[pair.Key] = true
			if keyMeta, ok := keys[pair.Key]; ok {
				if keyMeta.created == pair.CreateIndex {
					// Existing key that was seen before
					if keyMeta.subscriber != nil && keyMeta.modified != pair.ModifyIndex {
						// It's changed!
						keyMeta.modified = pair.ModifyIndex
						select {
						case <-done:
							return
						case keyMeta.subscriber <- pair:
						}
					}
					continue
				} else {
					// This key was deleted and recreated between queries
					if keyMeta.subscriber != nil {
						select {
						case <-done:
							return
						case keyMeta.subscriber <- nil:
						}
						close(keyMeta.subscriber)
					}
					// Fall through to re-create this key
				}
			}
			// Found a new key
			keyMeta := &keyMeta{
				created:    pair.CreateIndex,
				modified:   pair.ModifyIndex,
				subscriber: onNewKey(pair.Key),
			}
			keys[pair.Key] = keyMeta
			if keyMeta.subscriber != nil {
				select {
				case <-done:
					return
				case keyMeta.subscriber <- pair:
				}
			}
		}

		// Scan for deleted keys
		for key, keyMeta := range keys {
			if !visited[key] {
				if keyMeta.subscriber != nil {
					select {
					case <-done:
						return
					case keyMeta.subscriber <- nil:
					}
					close(keyMeta.subscriber)
				}
				delete(keys, key)
			}
		}
	}
}

type WatchedChanges struct {
	Created api.KVPairs
	Updated api.KVPairs
	Deleted api.KVPairs
}

// WatchDiff watches a Consul prefix for changes and categorizes them
// into create, update, and delete, please note that if a kvPair was
// create and modified before this starts watching, this watch will
// treat it as a create
func WatchDiff(
	prefix string,
	clientKV ConsulLister,
	quitCh <-chan struct{},
	outErrors chan<- error,
) <-chan *WatchedChanges {
	outCh := make(chan *WatchedChanges)

	go func() {
		defer close(outCh)

		// Keep track of what we have seen so that we know when something was changed
		keys := make(map[string]*api.KVPair)

		var currentIndex uint64
		timer := time.NewTimer(time.Duration(0))

		for {
			select {
			case <-quitCh:
				return
			case <-timer.C:
			}
			timer.Reset(250 * time.Millisecond) // upper bound on request rate

			pairs, queryMeta, err := SafeList(clientKV, quitCh, prefix, &api.QueryOptions{
				WaitIndex: currentIndex,
			})
			if err == CanceledError {
				select {
				case <-quitCh:
				case outErrors <- err:
				}
				return
			} else if err != nil {
				select {
				case <-quitCh:
				case outErrors <- err:
				}
				timer.Reset(2 * time.Second) // backoff
				continue
			}

			currentIndex = queryMeta.LastIndex
			// A copy used to keep track of what was deleted
			mapCopy := make(map[string]*api.KVPair)
			for key, val := range keys {
				mapCopy[key] = val
			}

			outgoingChanges := &WatchedChanges{}
			for _, val := range pairs {
				if _, ok := keys[val.Key]; !ok {
					// If it is not in the map, then it was a create
					outgoingChanges.Created = append(outgoingChanges.Created, val)
					keys[val.Key] = val

				} else if !bytes.Equal(keys[val.Key].Value, val.Value) {
					// If is in the map and the values are the not same, then it was an update
					// TODO: Should use something else other than comparing values
					outgoingChanges.Updated = append(outgoingChanges.Updated, val)
					if _, ok := mapCopy[val.Key]; ok {
						delete(mapCopy, val.Key)
					}
					keys[val.Key] = val

				} else {
					// Otherwise it is in the map and the values are equal, so it was not an update
					if _, ok := mapCopy[val.Key]; ok {
						delete(mapCopy, val.Key)
					}
				}
			}
			// If it was not observed, then it was a delete
			for key, val := range mapCopy {
				outgoingChanges.Deleted = append(outgoingChanges.Deleted, val)
				if _, ok := keys[key]; ok {
					delete(keys, key)
				}
			}

			select {
			case <-quitCh:
			case outCh <- outgoingChanges:
			}
		}
	}()

	return outCh
}
