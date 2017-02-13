/*
This package provides a binary that extracts data from commandline arguments as
well as process environment and writes exit information about a recently-exited
runit pod to a sqlite database for processing by the preparer.  See
(http://smarden.org/runit/runsv.8.html) for documentation on the ./finish file
for runit, within which this process will be invoked.
*/
package main

import (
	"log"
	"time"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/preparer/podprocess"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	exitCode     = kingpin.Arg("exitcode", "The exit code to be recorded to the sqlite database, automatically provided by runit's ./finish system").Required().Int()
	exitStatus   = kingpin.Arg("exitstatus", "The least significant byte of exit stasus as determined by waitpid(2), automatically provided by runit's ./finish system").Required().Int()
	databasePath = kingpin.Flag("database-path", "The path to the sqlite database to which finish information should be written").Required().ExistingFile()
	retries      = kingpin.Flag("retries", "The number of times to retry the database insert. This can avoid temporary database lock conflicts from multiple writers").Default("3").Int()
)

const (
	backoffTime = 2 * time.Second
)

func main() {
	kingpin.Parse()

	envExtractor := podprocess.EnvironmentExtractor{
		DatabasePath: *databasePath,
		Logger:       logging.DefaultLogger,
	}

	var err error
	// WriteFinish() is writing to a sqlite database that may be shared
	// across many pods across a physical host. The nature of sqlite is
	// that writes lock the entire database, which can cause transient
	// failures. To reduce the likelihood of writes fighting each other, we
	// retry writes a few times after they fail
	for i := 0; i < *retries; i++ {
		err = envExtractor.WriteFinish(*exitCode, *exitStatus)
		if !podprocess.IsRetryable(err) {
			break
		}

		time.Sleep(backoffTime)
	}

	if err != nil {
		log.Fatalf("Could not write finish information to database: %s", err)
	}
}
