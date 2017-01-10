/*
The store package is a product of the attempt to decouple P2 code from the
consul datastore. As of the time of this writing (12-20-2016), P2 interacts
with our consul datastore via interfaces and concrete types defined within
pkg/kp and its sub packages. The APIs exposed by the interfaces often contain
consul-specific concepts. For example, the SetPod() function on kp.consulStore
contains a PodPrefix argument, which maps directly onto the storage mechanism
in consul. For example, a lookup of consul key "intent/node.com/my_pod" means
calling Pod("intent", "node.com", "my_pod"), rather than the more high-level
GetIntentForPod("node.com", "my_pod").

The "store" package aims to define store types with functions that make sense
based on the logical structure of P2 data, and aim to mask any database
implementation details.

The ultimate goal is to make the various P2 components (e.g. p2-preparer,
p2-rctl-server) able to interact with a datastore directly or via an API server
using gRPC.
*/
package store
