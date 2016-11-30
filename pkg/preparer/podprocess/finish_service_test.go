package podprocess

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/types"

	_ "github.com/mattn/go-sqlite3"
)

func initFinishService(t *testing.T) (FinishService, string, func()) {
	// Make a temp dir to put the sqlite database in
	tempDir, err := ioutil.TempDir("", "finish_service_test")
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Could not create temp dir for migration test: %s", err)
	}

	dbPath := filepath.Join(tempDir, "test.db")

	finishService, err := NewSQLiteFinishService(dbPath, logging.DefaultLogger)
	if err != nil {
		os.RemoveAll(tempDir)
		finishService.Close()
		t.Fatalf("Unable to initialize finish service: %s", err)
	}

	err = finishService.Migrate()
	if err != nil {
		os.RemoveAll(tempDir)
		finishService.Close()
		t.Fatalf("Unexpected error running migration: %s", err)
	}

	return finishService, dbPath, func() {
		os.RemoveAll(tempDir)
	}
}

func TestMigrate(t *testing.T) {
	finishService, dbPath, closeFunc := initFinishService(t)
	defer closeFunc()
	defer finishService.Close()

	// now check that we can insert a finish to confirm finishes table was created
	err := finishService.Insert(FinishOutput{
		PodID:        "some_pod",
		PodUniqueKey: types.NewPodUUID(),
		LaunchableID: "some_launchable",
		EntryPoint:   "launch",
		ExitCode:     4,
		ExitStatus:   127,
	})
	if err != nil {
		t.Errorf("Could not insert a finish row, the finishes table might not have been created: %s", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Could not open database to confirm migrations worked: %s", err)
	}
	defer db.Close()

	// now check schema version
	var schemaVersion int64
	err = db.QueryRow(getSchemaVersionQuery).Scan(&schemaVersion)
	switch {
	case err == sql.ErrNoRows:
		t.Fatal("Schema version table was not written")
	case err != nil:
		t.Fatalf("Could not check schema version: %s", err)
	}

	if schemaVersion != int64(len(sqliteMigrations)) {
		t.Errorf("Expected schema_version to be %d but was %d", len(sqliteMigrations), schemaVersion)
	}
}

func TestPruneRowsBefore(t *testing.T) {
	finishService, dbPath, closeFunc := initFinishService(t)
	defer closeFunc()
	defer finishService.Close()

	// Insert 3 rows
	for i := 0; i < 3; i++ {
		// now check that we can insert a finish to confirm finishes table was created
		err := finishService.Insert(FinishOutput{
			PodID:        "some_pod",
			PodUniqueKey: types.NewPodUUID(),
			LaunchableID: "some_launchable",
			EntryPoint:   "launch",
			ExitCode:     4,
			ExitStatus:   127,
		})
		if err != nil {
			t.Errorf("Could not insert a finish row, the finishes table might not have been created: %s", err)
		}
	}

	// Open up the DB directly so we can mess with the dates
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Could not open database to confirm migrations worked: %s", err)
	}
	defer db.Close()

	// Set the dates of the first two rows to a week in the past
	_, err = db.Exec(`
	update finishes
	set date = ?
	where id < 3
	`, time.Now().Add(-7*24*time.Hour))
	if err != nil {
		t.Fatalf("Could not set time back a week for two rows: %s", err)
	}

	// Now prune any rows older than an hour, we should see the first two deleted
	err = finishService.PruneRowsBefore(time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("Unexpected error pruning rows: %s", err)
	}

	finishes, err := finishService.GetLatestFinishes(0)
	if err != nil {
		t.Fatalf("Could not fetch all finishes from the table to verify pruning: %s", err)
	}

	if len(finishes) != 1 {
		t.Fatalf("Expected only 1 row after pruning, but there were %d", len(finishes))
	}
}
