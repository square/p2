package roll

import (
	"testing"
	"time"

	"github.com/square/p2/pkg/audit"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/store/consul/auditlogstore"
	"github.com/square/p2/pkg/store/consul/consulutil"
	"github.com/square/p2/pkg/store/consul/rollstore"
)

func TestAuditLogCreation(t *testing.T) {
	fixture := consulutil.NewFixture(t)

	applicator := labels.NewConsulApplicator(fixture.Client, 0)
	logger := logging.TestLogger()
	rollStore := rollstore.NewConsul(fixture.Client, applicator, &logger)

	farm := &Farm{
		rls:     rollStore,
		txner:   fixture.Client.KV(),
		labeler: applicator,
		config: FarmConfig{
			ShouldCreateAuditLogRecords: true,
		},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		farm.mustDeleteRU("some_id", logger)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for RU deletion")
	}

	auditLogStore := auditlogstore.NewConsulStore(fixture.Client.KV())
	records, err := auditLogStore.List()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 {
		t.Fatalf("expected one audit log record to be created but there were %d", len(records))
	}

	for _, val := range records {
		if val.EventType != audit.RUCompletionEvent {
			t.Fatalf("expected audit log type to be %s but was %s", audit.RUCompletionEvent, val.EventType)
		}
	}
}
