package configstore

import "context"

type ID string
type Version string

type Fields struct {
	Config map[interface{}]interface{}
	ID     ID
}

type Storer interface {
	FetchConfig(ID) (Fields, Version, error)
	PutConfig(context.Context, ID, Fields, Version) error
	DeleteConfig(context.Context, ID, Version) error
}
