package auditlogstore

import (
	"encoding/json"
	"time"
)

const CurrentSchemaVersion int = 1

type EventType string
type SchemaVersion int
type ID string

func (i ID) String() string { return string(i) }

// AuditLog represents a stored value in consul expressing an event for which audit records
// are desired. An AuditLog consists of an event type, a json message with details which
// will have a different schema for each event type, a timestamp and a schema version.
type AuditLog struct {
	EventType     EventType        `json:"event_type"`
	EventDetails  *json.RawMessage `json:"event_details"`
	Timestamp     time.Time        `json:"timestamp"`
	SchemaVersion SchemaVersion    `json:"schema_version"`
}

// SchemaVersion implements MarshalJSON() so that every JSON representation of
// AuditLog has the correct schema version, even if the AuditLog struct has a
// different value set when it is marshaled
func (s SchemaVersion) MarshalJSON() ([]byte, error) {
	return json.Marshal(CurrentSchemaVersion)
}

var _ json.Marshaler = SchemaVersion(0)
