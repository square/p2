package util

import "github.com/square/p2/pkg/types"

type PodIntallationError struct {
	Inner error
	PodID types.PodID
}

func (r PodIntallationError) Error() string {
	return r.Inner.Error()
}

func IsPodIntallationError(err error) bool {
	_, ok := err.(PodIntallationError)
	return ok
}
