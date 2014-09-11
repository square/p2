pp-kv-consul
============

The platform requires an interface to a k/v store that provides a consistent
view of the data. It only needs to be available for changes. If it is not
available, applications running on the platform should be unaffected.

This implementation is backed by Consul.
