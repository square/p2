package consulutil

import (
	"bytes"
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/rcrowley/go-metrics"
	p2metrics "github.com/square/p2/pkg/metrics"
)

type WatchedKeys struct {
	Keys []string
	Err  error
}

// WatchKeys executes consul keys queries on a particular prefix and passes the
// set of keys on an output channel each time the query returns
func WatchKeys(
	prefix string,
	clientKV ConsulKeyser,
	done <-chan struct{},
	pause time.Duration,
) chan WatchedKeys {
	out := make(chan WatchedKeys)
	go func() {
		defer close(out)
		var currentIndex uint64
		timer := time.NewTimer(0)

		listLatencyHistogram, consulLatencyHistogram, outputPairsBlocking, _ := watchHistograms("keys")

		// put a lower bound on pause time to prevent tight looping queries against
		// consul
		if pause < 250*time.Millisecond {
			pause = 250 * time.Millisecond
		}

		// Don't allocate these in a loop
		var (
			listStart        time.Time
			outputPairsStart time.Time
		)

		for {
			select {
			case <-done:
				return
			case <-timer.C:
			}

			timer.Reset(pause)
			listStart = time.Now()
			keys, queryMeta, err := SafeKeys(clientKV, done, prefix, &api.QueryOptions{
				WaitIndex: currentIndex,
			})
			// outputPairsHistogram.Update(int64(sizeInBytes(keys)))
			if err == CanceledError {
				return
			} else if err != nil {
				select {
				case <-done:
				case out <- WatchedKeys{
					Err: err,
				}:
				}
				timer.Reset(2*time.Second + pause) // back off a little
				continue
			}

			consulLatencyHistogram.Update(int64(queryMeta.RequestTime))
			listLatencyHistogram.Update(int64(time.Since(listStart) / time.Millisecond))
			// This might happen if a watch expires on a node that was stale for a
			// long time.  Not likely but good to be careful.
			if queryMeta.LastIndex < currentIndex {
				select {
				case <-done:
					return
				case out <- WatchedKeys{
					Err: fmt.Errorf("watch returned last index of %d but asked for %d", queryMeta.LastIndex, currentIndex),
				}:
				}

				// try again
				continue
			}

			// nothing has changed since last time, just query
			// again
			if queryMeta.LastIndex == currentIndex {
				continue
			}

			currentIndex = queryMeta.LastIndex
			outputPairsStart = time.Now()
			select {
			case <-done:
				return
			case out <- WatchedKeys{
				Keys: keys,
			}:

				outputPairsBlocking.Update(int64(time.Since(outputPairsStart) / time.Millisecond))
			}
		}
	}()
	return out
}

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
	pause time.Duration,
) {
	defer close(outPairs)
	var currentIndex uint64
	timer := time.NewTimer(time.Duration(0))

	// Pause signifies the amount of time to wait after a result is
	// returned by the watch. Some use cases may want to respond quickly to
	// a change after a period of stagnation, but are able to tolerate a
	// degree of staleness in order to reduce QPS on the data store
	if pause < 250*time.Millisecond {
		pause = 250 * time.Millisecond
	}

	listLatencyHistogram, consulLatencyHistogram, outputPairsBlocking, outputPairsHistogram := watchHistograms("prefix")

	var (
		safeListStart    time.Time
		outputPairsStart time.Time
	)
	for {
		select {
		case <-done:
			return
		case <-timer.C:
		}
		timer.Reset(pause) // upper bound on request rate
		safeListStart = time.Now()
		pairs, queryMeta, err := List(clientKV, done, prefix, &api.QueryOptions{
			WaitIndex: currentIndex,
		})
		listLatencyHistogram.Update(int64(time.Since(safeListStart) / time.Millisecond))
		switch err {
		case CanceledError:
			return
		case nil:
			if queryMeta.LastIndex < currentIndex {
				// The "stale" query returned data that's older than what this watcher has
				// already seen. Ignore the old data.
				continue
			}
			currentIndex = queryMeta.LastIndex
			consulLatencyHistogram.Update(int64(queryMeta.RequestTime))
			outputPairsStart = time.Now()
			select {
			case <-done:
			case outPairs <- pairs:
			}
			outputPairsBlocking.Update(int64(time.Since(outputPairsStart) / time.Millisecond))
			outputPairsHistogram.Update(int64(sizeInBytes(pairs)))
		default:
			select {
			case <-done:
			case outErrors <- err:
			}
			timer.Reset(2*time.Second + pause) // backoff
		}
	}
}

func watchHistograms(watchType string) (listLatency metrics.Histogram, consulLatency metrics.Histogram, outputPairsWait metrics.Histogram, outputPairsBytes metrics.Histogram) {
	listLatency = metrics.GetOrRegisterHistogram(fmt.Sprintf("list_latency_%s", watchType), p2metrics.Registry, metrics.NewExpDecaySample(1028, 0.015))
	consulLatency = metrics.GetOrRegisterHistogram(fmt.Sprintf("consul_latency_%s", watchType), p2metrics.Registry, metrics.NewExpDecaySample(1028, 0.015))
	outputPairsWait = metrics.GetOrRegisterHistogram(fmt.Sprintf("output_pairs_wait_%s", watchType), p2metrics.Registry, metrics.NewExpDecaySample(1028, 0.015))
	outputPairsBytes = metrics.GetOrRegisterHistogram(fmt.Sprintf("output_pairs_bytes_%s", watchType), p2metrics.Registry, metrics.NewExpDecaySample(1028, 0.015))

	return
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

	listLatencyHistogram, consulLatencyHistogram, outputPairsBlocking, _ := watchHistograms("single")

	var (
		safeGetStart    time.Time
		outputPairStart time.Time
	)

	for {
		select {
		case <-done:
			return
		case <-timer.C:
		}
		safeGetStart = time.Now()
		timer.Reset(250 * time.Millisecond) // upper bound on request rate
		kvp, queryMeta, err := Get(clientKV, done, key, &api.QueryOptions{
			WaitIndex: currentIndex,
		})
		listLatencyHistogram.Update(int64(time.Since(safeGetStart) / time.Millisecond))
		switch err {
		case CanceledError:
			return
		case nil:
			outputPairStart = time.Now()
			consulLatencyHistogram.Update(int64(queryMeta.RequestTime))
			currentIndex = queryMeta.LastIndex
			select {
			case <-done:
			case outKVP <- kvp:
			}
			outputPairsBlocking.Update(int64(time.Since(outputPairStart) / time.Millisecond))
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
	Same    api.KVPairs
}

// WatchDiff watches a Consul prefix for changes and categorizes them
// into create, update, and delete, please note that if a kvPair was
// create and modified before this starts watching, this watch will
// treat it as a create
func WatchDiff(
	prefix string,
	clientKV ConsulLister,
	quitCh <-chan struct{},
) (<-chan *WatchedChanges, <-chan error) {
	outCh := make(chan *WatchedChanges)
	outErrors := make(chan error)

	// initialized tracks whether we've done a loop iteration yet. For the first iteration, we don't want to
	// do a stale query of consul to ensure the caller of WatchDiff() doesn't get a more stale result
	// than in a previous watch. Once we've initialized state with the last index from a consistent query
	// we can then rely on stale queries and discard values which have a lower index.
	initialized := false
	go func() {
		defer close(outCh)
		defer close(outErrors)

		// Keep track of what we have seen so that we know when something was changed
		keys := make(map[string]*api.KVPair)

		var currentIndex uint64
		timer := time.NewTimer(time.Duration(0))

		listLatencyHistogram, consulLatencyHistogram, outputPairsBlocking, outputPairsHistogram := watchHistograms("diff")

		var (
			safeListStart    time.Time
			outputPairsStart time.Time
		)
		for {
			select {
			case <-quitCh:
				return
			case <-timer.C:
			}
			timer.Reset(250 * time.Millisecond) // upper bound on request rate

			safeListStart = time.Now()
			pairs, queryMeta, err := List(clientKV, quitCh, prefix, &api.QueryOptions{
				WaitIndex: currentIndex,
			})
			listLatencyHistogram.Update(int64(time.Since(safeListStart) / time.Millisecond))

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

			if queryMeta.LastIndex < currentIndex {
				// The "stale" query returned data that's older than what this watcher has
				// already seen. Ignore the old data.
				continue
			}
			consulLatencyHistogram.Update(int64(queryMeta.RequestTime))
			currentIndex = queryMeta.LastIndex
			initialized = true
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
					outgoingChanges.Same = append(outgoingChanges.Same, val)
				}
			}
			// If it was not observed, then it was a delete
			for key, val := range mapCopy {
				outgoingChanges.Deleted = append(outgoingChanges.Deleted, val)
				if _, ok := keys[key]; ok {
					delete(keys, key)
				}
			}

			outputPairsStart = time.Now()
			select {
			case <-quitCh:
			case outCh <- outgoingChanges:
			}
			outputPairsBlocking.Update(int64(time.Since(outputPairsStart) / time.Millisecond))
			outputPairsHistogram.Update(int64(sizeInBytes(pairs)))
		}
	}()

	return outCh, outErrors
}

func sizeInBytes(pairs api.KVPairs) int {
	size := 0
	for _, pair := range pairs {
		size += len(pair.Value)
	}
	return size
}
