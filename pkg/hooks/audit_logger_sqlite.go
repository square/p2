package hooks

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"
)

type SQLiteAuditLogger struct {
	sqlite *sql.DB
	logger *logging.Logger
}

func NewSQLiteAuditLogger(sqliteDBPath string, logger *logging.Logger) (*SQLiteAuditLogger, error) {
	db, err := sql.Open("sqlite3", sqliteDBPath)
	if err != nil {
		return nil, util.Errorf("Unable to open sqlite at %s. %v", sqliteDBPath, err)
	}
	if db == nil {
		return nil, util.Errorf("Unable to create a sqlite connection.")
	}
	al := &SQLiteAuditLogger{
		sqlite: db,
		logger: logger,
	}
	if err := al.ensureMigrated(); err != nil {
		return nil, util.Errorf("Failed to apply migrations to sqlite DB: %v", err)
	}
	return al, nil
}

func (al *SQLiteAuditLogger) log(ctx *HookExecContext, success bool) {
	env := ctx.env
	stmt := `INSERT INTO hook_results(
pod_id,
pod_unique_key,
hook_name,
hook_stage,
success
) VALUES(?,?,?,?,?)`
	podID := env.HookedPodIDEnvVar
	podUniqueKey := env.HookedPodUniqueKeyEnvVar
	hookName := ctx.Name
	hookStage := env.HookEventEnvVar
	// Boolean values are represented by TINYINTs in SQL. Adapt.
	dbSuccess := 0
	if !success {
		dbSuccess = 1
	}

	_, err := al.sqlite.Exec(stmt, podID, podUniqueKey, hookName, hookStage, dbSuccess)
	if err != nil {
		al.logger.Errorln("nope: %v", err)
	}
}

func (al *SQLiteAuditLogger) LogSuccess(ctx *HookExecContext) {
	al.log(ctx, true)
}

func (al *SQLiteAuditLogger) LogFailure(ctx *HookExecContext, err error) {
	al.log(ctx, false)
}

var (
	sqliteMigrations = []string{
		`create table hook_results (
	      id integer not null primary key autoincrement,
	      date datetime default current_timestamp,
	      pod_id text,
	      pod_unique_key text,
	      hook_name string,
	      hook_stage string,
	      success tinyint);`,

		"create index hook_results_hook_name on hook_results(hook_name);",
		"create index hook_results_pod_id on hook_results(pod_id);",
		// FUTURE MIGRATIONS GO HERE
	}
)

const (
	sqliteCreateSchemaVersionTable = `create table if not exists hooks_schema_version ( version integer );`
	getSchemaVersionQuery          = `select version from hooks_schema_version;`
	updateSchemaVersionStatement   = `update hooks_schema_version set version = ?;`
)

// Close will terminate this AuditLogger. Re-establishing the connection is not supported, use the constructor.
func (al *SQLiteAuditLogger) Close() error {
	return al.sqlite.Close()
}

func (al *SQLiteAuditLogger) ensureMigrated() error {
	_, err := al.sqlite.Exec(sqliteCreateSchemaVersionTable)
	if err != nil {
		return err
	}

	var lastSchemaVersion int64
	err = al.sqlite.QueryRow(getSchemaVersionQuery).Scan(&lastSchemaVersion)
	switch {
	case err == sql.ErrNoRows:
		_, err := al.sqlite.Exec(sqliteCreateSchemaVersionTable)
		if err != nil {
			return util.Errorf("Unable to initialize schema_version table: %s", err)
		}
	case err != nil:
		return err
	}

	tx, err := al.sqlite.Begin()
	if err != nil {
		return util.Errorf("Could not start transaction for migrations: %s", err)
	}

	defer func() {
		if err == nil {
			// return the commit error by assigning to return variable
			err = tx.Commit()
		} else {
			// return the original error not the rollback error
			_ = tx.Rollback()
		}
	}()

	for i := lastSchemaVersion; i < int64(len(sqliteMigrations)); i++ {
		statement := sqliteMigrations[i]
		_, err = tx.Exec(statement)
		if err != nil {
			return util.Errorf("Could not apply migration %d: %s", i+1, err)
		}
	}

	_, err = tx.Exec(updateSchemaVersionStatement, int64(len(sqliteMigrations)))
	if err != nil {
		al.logger.WithError(err).Errorln("Could not update schema_version table")
		return err
	}

	return nil
}
