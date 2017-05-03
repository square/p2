package auditlogstore

import (
	"encoding/json"
	"testing"
)

func TestSchemaVersionMarshal(t *testing.T) {
	a := AuditLog{
		SchemaVersion: -1000,
	}
	jsonBytes, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("could not marshal audit log as JSON: %s", err)
	}

	err = json.Unmarshal(jsonBytes, &a)
	if err != nil {
		t.Fatalf("could not unmarshal audit log from JSON: %s", err)
	}

	if int(a.SchemaVersion) != CurrentSchemaVersion {
		t.Errorf("expected all audit log records to marshal with schema version %d, but this one had %d", CurrentSchemaVersion, a.SchemaVersion)
	}
}
