package podprocess

import (
	"database/sql"
	"time"

	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	_ "github.com/mattn/go-sqlite3"
)

type FinishService interface {
	// Closes any resources such as database connection
	Close() error
	// Inserts a finish row corresponding to a process exit
	Insert(finish FinishOutput) error
	// Runs any outstanding migrations
	Migrate() error
	// Reads all finish data after the given ID
	GetLatestFinishes(lastID int64) ([]FinishOutput, error)
	// Gets the last finish result for a given PodUniqueKey
	LastFinishForPodUniqueKey(podUniqueKey types.PodUniqueKey) (FinishOutput, error)
}

type sqliteFinishService struct {
	db     *sql.DB
	logger logging.Logger
}

func NewSQLiteFinishService(sqliteDBPath string, logger logging.Logger) (FinishService, error) {
	db, err := sql.Open("sqlite3", sqliteDBPath)
	if err != nil {
		return nil, util.Errorf("Could not open database: %s", err)
	}

	return sqliteFinishService{
		db:     db,
		logger: logger,
	}, nil
}

// Represents a row in the sqlite database indicating the exit of a runit process.
type FinishOutput struct {
	PodID        types.PodID         `json:"pod_id"`
	LaunchableID launch.LaunchableID `json:"launchable_id"`
	EntryPoint   string              `json:"entry_point"`
	PodUniqueKey types.PodUniqueKey  `json:"pod_unique_key"`

	// These two model the arguments given to the ./finish script under runit:
	// (http://smarden.org/runit/runsv.8.html)
	ExitCode   int `json:"exit_code"`
	ExitStatus int `json:"exit_status"`

	// This is never written explicitly and is determined automatically by
	// sqlite (via AUTOINCREMENT)
	ID int64

	// This is never written explicitly, it's determined automatically by
	// sqlite (via DEFAULT CURRENT_TIMESTAMP)
	ExitTime time.Time `json:"exit_time"`
}

func (s sqliteFinishService) Insert(finish FinishOutput) error {
	stmt := `insert into finishes(
		    pod_id,
		    pod_unique_key,
		    launchable_id,
		    entry_point,
		    exit_code,
		    exit_status
		  ) VALUES(?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(stmt,
		finish.PodID.String(),
		finish.PodUniqueKey.String(),
		finish.LaunchableID.String(),
		finish.EntryPoint,
		finish.ExitCode,
		finish.ExitStatus,
	)
	if err != nil {
		return util.Errorf("Couldn't insert finish line into sqlite database: %s", err)
	}

	return nil
}

// Not considered a migration
const (
	getSchemaVersionQuery        = `select version from schema_version;`
	updateSchemaVersionStatement = `update schema_version set version = ?;`

	// This will always be run, and is idempotent
	sqliteCreateSchemaVersionTable = `create table if not exists schema_version ( version integer );`

	// This should only be run if no rows are returned when checking for the
	// written schema_version, which should only happen if the schema
	//version table was just created
	sqliteInitializeSchemaVersionTable = `insert into schema_version(version) values ( 0 );`
)

var (
	sqliteMigrations = []string{
		`create table finishes (
	    id integer not null primary key autoincrement,
	    date datetime default current_timestamp,
	    pod_id text,
	    pod_unique_key text,
	    launchable_id text,
	    entry_point text,
	    exit_code integer,
	    exit_status integer
	);`,

		// FUTURE MIGRATIONS GO HERE
	}
)

func (s sqliteFinishService) Migrate() (err error) {
	// idempotent
	_, err = s.db.Exec(sqliteCreateSchemaVersionTable)
	if err != nil {
		return util.Errorf("Could not set up schema_version table: %s", err)
	}

	var lastSchemaVersion int64
	err = s.db.QueryRow(getSchemaVersionQuery).Scan(&lastSchemaVersion)
	switch {
	case err == sql.ErrNoRows:
		// We just created the table, insert a row with 0
		_, err = s.db.Exec(sqliteInitializeSchemaVersionTable)
		if err != nil {
			return util.Errorf("Could not initialize schema_version table: %s", err)
		}
	case err != nil:
		return util.Errorf("Error checking schema version: %s", err)
	}

	if lastSchemaVersion == int64(len(sqliteMigrations)) {
		// we're caught up
		return nil
	}

	tx, err := s.db.Begin()
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
		s.logger.WithError(err).Errorln("Could not update schema_version table")
	}

	return err
}

func (s sqliteFinishService) Close() error {
	return s.db.Close()
}

func (f sqliteFinishService) GetLatestFinishes(lastID int64) ([]FinishOutput, error) {
	rows, err := f.db.Query(`
	    SELECT id, date, pod_id, pod_unique_key, launchable_id, entry_point, exit_code, exit_status
	    FROM finishes
	    WHERE id > ?
	    `, lastID)
	if err != nil {
		f.logger.WithError(err).Errorln("Could not query for latest process exits")
		return nil, err
	}
	defer rows.Close()

	var finishes []FinishOutput
	for rows.Next() {
		finishOutput, err := scanRow(rows)
		if err != nil {
			f.logger.WithError(err).Errorln("Could not scan row")
			return nil, err
		}

		finishes = append(finishes, finishOutput)
	}
	return finishes, nil
}

func (f sqliteFinishService) LastFinishForPodUniqueKey(podUniqueKey types.PodUniqueKey) (FinishOutput, error) {
	row := f.db.QueryRow(`
  SELECT id, date, pod_id, pod_unique_key, launchable_id, entry_point, exit_code, exit_status
  FROM finishes
  WHERE pod_unique_key = ?
  `, podUniqueKey.String())
	return scanRow(row)
}

// Implemented by both *sql.Row and *sql.Rows
type Scanner interface {
	Scan(...interface{}) error
}

// Runs Scan() once on the passed Scanner and converts the result to a FinishOutput
func scanRow(scanner Scanner) (FinishOutput, error) {
	var id int64
	var date time.Time
	var podID, podUniqueKey, launchableID, entryPoint string
	var exitCode, exitStatus int

	err := scanner.Scan(&id, &date, &podID, &podUniqueKey, &launchableID, &entryPoint, &exitCode, &exitStatus)
	if err != nil {
		return FinishOutput{}, err
	}

	return FinishOutput{
		ID:           id,
		PodID:        types.PodID(podID),
		LaunchableID: launch.LaunchableID(launchableID),
		EntryPoint:   entryPoint,
		PodUniqueKey: types.PodUniqueKey(podUniqueKey),
		ExitCode:     exitCode,
		ExitStatus:   exitStatus,
		ExitTime:     date,
	}, nil
}
