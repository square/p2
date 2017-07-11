package audit

import (
	"encoding/json"

	"github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/util"
)

const (
	// DSCreatedEvent signifies that the daemon set was created.
	DSCreatedEvent EventType = "DAEMON_SET_CREATED"

	// DSManifestUpdatedEvent signifies that the daemon set had its pod
	// manifest modified. This typically kicks off a replication to update
	// the manifests for all the nodes matched by the daemon set's node
	// selector
	DSManifestUpdatedEvent EventType = "DAEMON_SET_MANIFEST_UPDATED"

	// DSNodeSelectorUpdatedEvent signifies that the node selector of the
	// daemon set was changed. This will result in adding the daemon set's
	// pod manifest to new nodes that are matched, or removing it from
	// nodes that are no longer matched.
	DSNodeSelectorUpdatedEvent EventType = "DAEMON_SET_NODE_SELECTOR_UPDATED"

	// DSDeletedEvent signifies that the daemon set was deleted
	DSDeletedEvent EventType = "DAEMON_SET_DELETED"

	// DSEnabledEvent signifies that the daemon set was enabled. This might
	// kick off a manifest update to the nodes matched by the daemon set's
	// manifest
	DSEnabledEvent EventType = "DAEMON_SET_ENABLED"

	// DSEnabledEvent signifies that the daemon set was enabled. This might
	// pause a manifest update to the nodes matched by the daemon set's
	// manifest
	DSDisabledEvent EventType = "DAEMON_SET_DISABLED"

	// DSModifiedEvent signifies an update to the daemon set that doesn't
	// fit in any of the other event types. For example, changing the
	// timeout value will result in an event of this type (because it's
	// neither a manifest or "disabled" update)
	DSModifiedEvent EventType = "DAEMON_SET_MODIFIED"
)

// DsEventDetails defines a JSON structure for the details related to a daemon
// set event. For now the schema is the same for every event type but this may
// change in the future
//
type DSEventDetails struct {
	// DaemonSet is the daemon set that resulted from the event (e.g. after
	// the update was applied) for all event types other than deletions. In
	// the case of deletions, it will be the contents of the daemon set
	// BEFORE deletion
	DaemonSet fields.DaemonSet `json:"daemon_set"`

	// User represents the name of the user who executed the action to
	// which the event record pertains
	User string `json:"user"`
}

func NewDaemonSetDetails(ds fields.DaemonSet, user string) (json.RawMessage, error) {
	event := DSEventDetails{
		DaemonSet: ds,
		User:      user,
	}

	detailBytes, err := json.Marshal(event)
	if err != nil {
		return nil, util.Errorf("could not marshal DS event as json: %s", err)
	}

	return json.RawMessage(detailBytes), nil
}
