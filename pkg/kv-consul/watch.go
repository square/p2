package ppkv

import (
	"time"

	"github.com/armon/consul-api"
)

type Health interface {
	Service(service, tag string, passingOnly bool, q *consulapi.QueryOptions) ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error)
}

type KV interface {
	List(string, *consulapi.QueryOptions) (consulapi.KVPairs, *consulapi.QueryMeta, error)
}

// Modified code stolen from https://github.com/ryanbreen/fsconsul/blob/master/watch.go#L172
func WatchKV(
	kv KV,
	prefix string,
	opts consulapi.QueryOptions, // pass-by-value since we're going to be modifying it
	pairCh chan<- consulapi.KVPairs,
	errCh chan<- error,
	quitCh <-chan struct{}) {

	// Get the initial list of k/v pairs. We don't do a retryableList
	// here because we want a fast fail if the initial request fails.
	pairs, meta, err := kv.List(prefix, &opts)
	if err != nil {
		errCh <- err
		return
	}

	// Send the initial list out right away
	pairCh <- pairs

	// Loop forever (or until quitCh is closed) and watch the keys
	// for changes.
	curIndex := meta.LastIndex
	for {
		select {
		case <-quitCh:
			return
		default:
		}

		pairs, meta, err = retryableList(
			func() (consulapi.KVPairs, *consulapi.QueryMeta, error) {
				opts.WaitIndex = curIndex
				return kv.List(prefix, &opts)
			})

		if err != nil {
			errCh <- err
		} else {

			pairCh <- pairs
			curIndex = meta.LastIndex
		}
	}
}

// This function is able to call KV listing functions and retry them.
// We want to retry if there are errors because it is safe (GET request),
// and erroring early is MUCH more costly than retrying over time and
// delaying the configuration propagation.
func retryableList(f func() (consulapi.KVPairs, *consulapi.QueryMeta, error)) (consulapi.KVPairs, *consulapi.QueryMeta, error) {
	i := 0
	for {
		p, m, e := f()
		if e != nil {
			if i >= 3 {
				return nil, nil, e
			}
			i++
			// Reasonably arbitrary sleep to just try again... It is
			// a GET request so this is safe.
			time.Sleep(time.Duration(i*2) * time.Second)
		}
		return p, m, e
	}
}

// copypasta ahead. #ineedgenerics
func WatchServiceHealth(
	health Health,
	serviceID string,
	tag string,
	passingOnly bool,
	opts consulapi.QueryOptions, // pass-by-value since we're going to be modifying it
	healthCh chan<- []*consulapi.ServiceEntry,
	errCh chan<- error,
	quitCh <-chan struct{}) {

	// Get the initial list of healthchecks
	healthChecks, meta, err := health.Service(serviceID, tag, passingOnly, &opts)
	if err != nil {
		errCh <- err
		return
	}

	// Send the initial list out right away
	healthCh <- healthChecks

	// Loop forever (or until quitCh is closed) and watch the keys
	// for changes.
	curIndex := meta.LastIndex
	for {
		select {
		case <-quitCh:
			return
		default:
		}

		healthChecks, meta, err = retryServiceCheck(
			func() ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
				opts.WaitIndex = curIndex
				return health.Service(serviceID, tag, passingOnly, &opts)
			})

		if err != nil {
			errCh <- err
		} else {

			healthCh <- healthChecks
			curIndex = meta.LastIndex
		}
	}

}

func retryServiceCheck(f func() ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error)) ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
	i := 0
	for {
		p, m, e := f()
		if e != nil {
			if i >= 3 {
				return nil, nil, e
			}
			i++
			// Reasonably arbitrary sleep to just try again... It is
			// a GET request so this is safe.
			time.Sleep(time.Duration(i*2) * time.Second)
		}
		return p, m, e
	}
}
