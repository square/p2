package podprocess

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/types"

	_ "github.com/mattn/go-sqlite3"
)

func TestMigrate(t *testing.T) {
	// Make a temp dir to put the sqlite database in
	tempDir, err := ioutil.TempDir("", "test_migrate")
	if err != nil {
		t.Fatalf("Could not create temp dir for migration test: %s", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test_migrate.db")

	finishService, err := NewSQLiteFinishService(dbPath, logging.DefaultLogger)
	if err != nil {
		t.Fatalf("Unable to initialize finish service: %s", err)
	}

	err = finishService.Migrate()
	if err != nil {
		t.Fatalf("Unexpected error running migration: %s", err)
	}

	// now check that we can insert a finish to confirm finishes table was created
	err = finishService.Insert(FinishOutput{
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
