/*
Package store is a work-in-progress package that gives access to data storage
for P2 objects. As of the writing of this comment (12-22-16) there's nothing
here, because up until this point all of our storage code is consul-specific
and consul-specific semantics have leaked into our APIs.

Any new storage code should have its interface expressed in terms of P2 types
only, e.g. GetPodIntent(types.NodeName, types.PodID) (manifest.Manifest, error)
instead of Pod(types.PodPrefix, types.NodeName, types.PodID)
(manifest.Manifest, error), which exposes "pod prefix" which directly maps to a
consul keyspace (e.g. "/intent" or "/reality").

The idea is that any code that imports a type from pkg/store can easily switch
backends.

For example, imagine a pkg/foo that defines an RCGetter interface and a Foo type:

type RCGetter interface {
  GetRC(rc_fields.ID) (rc_fields.ReplicationController, error)
}

func New(RCGetter) Foo { ... }

With this scheme, code instantiating a Foo can easily choose a backend to use,
e.g. by using foo.New(store.NewConsul()), foo.New(store.NewETCD()), or
foo.New(store.NewFancyGRPCClient()).
*/
package store
