/*
This package provides a Reporter which scans a sqlite database for process exit
information, writing it to the consul datastore. This is useful for allowing
external systems to examine the success or failure of a pod.
*/
package podprocess

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/square/p2/pkg/kp/statusstore/podstatus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
)

const (
	workspaceFileName     = "last_synced_finish_id"
	workspaceTempFileName = "last_synced_finish_id.tmp"

	// Specifies the amount of time to wait between SQLite queries for the latest finish events
	DefaultPollInterval = 15 * time.Second
)

type ReporterConfig struct {
	// Path to the sqlite database that should be polled for finish
	// information.  NOTE: the written file AND THE DIRECTORY IT'S IN will
	// be given file perms 0666, because sqlite works like that.  you can't
	// write to a database unless you can also write to the directory it's
	// in. As a result, the path here should include at least one level of
	// directory to be created
	SQLiteDatabasePath string `yaml:"sqlite_database_path"`

	// Path to the executable that constructs finish information based on process environment
	EnvironmentExtractorPath string `yaml:"environment_extractor_path"`

	// Path to a file the pod process reporter can use as a workspace. It's used to store the most recently
	// recorded database id, and needs to persist across p2-preparer restarts
	WorkspaceDirPath string `yaml:"workspace_dir_path"`

	// e.g. to /usr/bin/timeout. This is useful because the finish script
	// blocks restart of runit processes, so it's recommended to wrap the
	// database insert in a timeout.
	TimeoutPath string `yaml:"timeout_path"`
}

func (r ReporterConfig) FinishExec() []string {
	var ret []string
	if r.TimeoutPath != "" {
		ret = append(ret, r.TimeoutPath, "10")
	}

	ret = append(
		ret,
		r.EnvironmentExtractorPath,
		"$1",
		"$2",
		"--database-path",
		r.SQLiteDatabasePath,
	)
	return ret
}

// Returns true if the contents of the ReporterConfig indicate that a pod
// process reporter should be run. In other words, if it returns true, New()
// should be called.
func (r ReporterConfig) FullyConfigured() bool {
	return r.SQLiteDatabasePath != "" &&
		r.EnvironmentExtractorPath != "" &&
		r.WorkspaceDirPath != ""
}

type Reporter struct {
	// Abstracts database operations
	finishService            FinishService
	environmentExtractorPath string
	workspaceDirPath         string

	logger         logging.Logger
	podStatusStore podstatus.Store
	pollInterval   time.Duration

	timeoutPath string

	// We need to keep this around only so we can chmod the file after we
	// migrate, because sqlite doesn't actually write the file until
	// something (e.g. table schema) is written
	databasePath string
}

// Should only be called if config.FullyConfigured() returned true.
// Returns an error iff there is a configuration problem.
func New(config ReporterConfig, logger logging.Logger, podStatusStore podstatus.Store, pollInterval time.Duration) (*Reporter, error) {
	if config.SQLiteDatabasePath == "" {
		// If the caller uses config.FullyConfigured() properly, this shouldn't happen
		return nil, util.Errorf("sqlite_database_path not configured, process exit status will not be captured")
	}

	info, err := os.Stat(config.EnvironmentExtractorPath)
	if err != nil {
		return nil, util.Errorf("Could not stat environment_extractor_path: %s", err)
	}

	// Check that the file is executable by root
	if info.Mode()&0100 != 0100 {
		return nil, util.Errorf("%s is not executable: perms were %s", config.EnvironmentExtractorPath, info.Mode())
	}

	if pollInterval == 0 {
		pollInterval = DefaultPollInterval
	}

	// The directory the sqlite database is in needs to be world readable
	// and writable if the database is to be. However, if the directory the
	// database is in is executable we want to keep it that way.
	databaseDirPath := filepath.Dir(config.SQLiteDatabasePath)
	dirInfo, err := os.Stat(databaseDirPath)
	switch {
	case os.IsNotExist(err):
		err = os.MkdirAll(databaseDirPath, 0777)
		if err != nil {
			return nil, util.Errorf("Could not create directory for sqlite database at '%s': %s", databaseDirPath, err)
		}

		// Chmod it again in case our umask caused the directory perms
		// to be different than 0777
		err = os.Chmod(databaseDirPath, 0777)
		if err != nil {
			return nil, util.Errorf("Could not chmod 0777 '%s': %s", databaseDirPath, err)
		}
	case err == nil:
		desiredPerms := dirInfo.Mode() | 0777
		err = os.Chmod(databaseDirPath, desiredPerms)
		if err != nil {
			return nil, util.Errorf("Could not make database directory %s world readable and writable: %s", databaseDirPath, err)
		}
	case err != nil:
		return nil, util.Errorf("Could not stat database directory %s: %s", databaseDirPath, err)
	}

	finishService, err := NewSQLiteFinishService(config.SQLiteDatabasePath, logger)
	if err != nil {
		return nil, err
	}

	return &Reporter{
		finishService:            finishService,
		environmentExtractorPath: config.EnvironmentExtractorPath,
		workspaceDirPath:         config.WorkspaceDirPath,
		logger:                   logger,
		podStatusStore:           podStatusStore,
		pollInterval:             pollInterval,
		timeoutPath:              config.TimeoutPath,
		databasePath:             config.SQLiteDatabasePath,
	}, nil
}

// Starts the reporter. Quickly returns an error if a startup issue occurs.
func (r *Reporter) Run(quitCh <-chan struct{}) error {
	err := r.initWorkspaceDir()
	if err != nil {
		_ = r.finishService.Close()
		return err
	}

	err = r.finishService.Migrate()
	if err != nil {
		_ = r.finishService.Close()
		return err
	}

	// The finish process doesn't run as root, so we need to chmod the SQL database to 666
	err = os.Chmod(r.databasePath, 0666)
	if err != nil {
		return util.Errorf("Could not chmod finish database %s to 0666 to allow finish to write to it: %s", r.databasePath, err)
	}

	go r.publishProcessExits(quitCh)
	return nil
}

// Initializes the workspace dir:
// 1) create it if it doesn't exist with perms 0600
// 2) create the workspace file inside of the directory if it doesn't exist (and write 0 value to it)
func (r *Reporter) initWorkspaceDir() error {
	// Create the dir if it doesn't exist
	dirInfo, err := os.Stat(r.workspaceDirPath)
	if os.IsNotExist(err) {
		err = os.Mkdir(r.workspaceDirPath, 0600)
		if err != nil {
			return util.Errorf("Could not create workspace dir: %s", err)
		}
	} else if err != nil {
		return util.Errorf("Could not stat workspace directory: %s", err)
	} else {
		if !dirInfo.IsDir() {
			return util.Errorf("Configured worskpace directory %s already exists but is not a directory", r.workspaceDirPath)
		}
	}

	workspaceFilePath := r.workspaceFilePath()
	// Check that the workspace file either:
	// 1) exists and is readable and writable by root
	// OR
	// 2) does not exist and is successfully created.
	info, err := os.Stat(workspaceFilePath)
	if os.IsNotExist(err) {
		file, err := os.Create(workspaceFilePath)
		if err != nil {
			return util.Errorf("Could not create workspace file at %s: %s", workspaceFilePath, err)
		}

		err = file.Chmod(0600)
		if err != nil {
			return util.Errorf("Could not chmod workspace file at %s: %s", workspaceFilePath, err)
		}

		// Initialize the file with an id of 0
		err = r.writeLastID(0)
		if err != nil {
			return err
		}
	} else if err != nil {
		return util.Errorf("Error running stat() on %s: %s", workspaceFilePath, err)
	} else {
		if info.Mode()&0600 != 0600 {
			return util.Errorf("Workspace file %s exists but is not readable and writable by root: perms %s", workspaceFilePath, info.Mode())
		}
	}

	return nil
}

func (r *Reporter) publishProcessExits(quitCh <-chan struct{}) {
	defer r.finishService.Close()
	timer := time.NewTimer(0)
	for {
		select {
		case <-quitCh:
			return
		case <-timer.C:
			r.reportLatestExits()
			timer.Reset(r.pollInterval)
		}
	}
}

func (r *Reporter) reportLatestExits() {
	lastID, err := r.getLastID()
	if err != nil {
		r.logger.WithError(err).Errorln("Could not read last ID from process exit sqlite db")
		return
	}

	finishes, err := r.finishService.GetLatestFinishes(lastID)
	if err != nil {
		// The error was already logged in GetLatestFinishes()
		return
	}

	defer func() {
		err = r.writeLastID(lastID)
		if err != nil {
			r.logger.WithError(err).Errorln("Could not write last ID to workspace file, duplicate messages may be reported")
		}
	}()

	for _, finish := range finishes {
		subLogger := r.logger.SubLogger(logrus.Fields{
			"pod_id":         finish.PodID,
			"launchable_id":  finish.LaunchableID,
			"entry_point":    finish.EntryPoint,
			"pod_unique_key": finish.PodUniqueKey,
			"exit_code":      finish.ExitCode,
			"exit_status":    finish.ExitStatus,
			"finish_id":      finish.ID,
			"exit_time":      finish.ExitTime,
		})
		subLogger.Infoln("Received process exit information")

		if finish.PodUniqueKey == "" {
			// Status is only written to consul for uuid pods
			lastID = finish.ID
			continue
		}

		err = r.podStatusStore.SetLastExit(finish.PodUniqueKey, finish.LaunchableID, finish.EntryPoint, podstatus.ExitStatus{
			ExitTime:   finish.ExitTime,
			ExitCode:   finish.ExitCode,
			ExitStatus: finish.ExitStatus,
		})
		if err != nil {
			subLogger.WithError(err).Errorln("Failed to record status")
			return
		}

		subLogger.Infoln("Successfully recorded status")
		lastID = finish.ID
	}
}

// Atomically updates the contents of r.WorkspacePath to contain the id
// (primary key) last read and processed from the sqlite database. Writes the
// value to a new file and then uses os.Rename() so that an intermediate error
// doesn't cause data to be lost.
//
// Not threadsafe.
func (r *Reporter) writeLastID(id int64) error {
	tmpFile, err := os.Create(r.workspaceTempFilePath())
	if err != nil {
		return err
	}

	stringToWrite := strconv.FormatInt(id, 10)
	_, err = tmpFile.WriteString(stringToWrite)
	if err != nil {
		return util.Errorf("Could not write newest ID to temporary file: %s", err)
	}

	err = os.Rename(r.workspaceTempFilePath(), r.workspaceFilePath())
	if err != nil {
		return util.Errorf("Could not rename temp file over workspace file: %s", err)
	}
	return nil
}

func (r *Reporter) getLastID() (int64, error) {
	bytes, err := ioutil.ReadFile(r.workspaceFilePath())
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(string(bytes), 10, 64)
}

func (r *Reporter) workspaceFilePath() string {
	return filepath.Join(r.workspaceDirPath, workspaceFileName)
}

func (r *Reporter) workspaceTempFilePath() string {
	return filepath.Join(r.workspaceDirPath, workspaceTempFileName)
}
