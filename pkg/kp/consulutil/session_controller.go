package consulutil

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/consul/api"

	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/util/param"
)

// SessionRetrySeconds specifies how long to wait between retries when establishing a
// session to Consul.
var SessionRetrySeconds = param.Int("session_retry_seconds", 5)

// SessionManager continually creates and maintains Consul sessions. It is intended to be
// run in its own goroutine. If one session expires, a new one will be created. As
// sessions come and go, the session ID (or "" for an expired session) will be sent on the
// output channel.
//
// Parameters:
//   config:  Configuration passed to Consul when creating a new session.
//   client:  The Consul client to use.
//   output:  The channel used for exposing Consul session IDs. This method takes
//            ownership of this channel and will close it once no new IDs will be created.
//   done:    Close this channel to close the current session (if any) and stop creating
//            new sessions.
//   logger:  Errors will be logged to this logger.
func SessionManager(
	config api.SessionEntry,
	client ConsulClient,
	output chan<- string,
	done chan struct{},
	logger logging.Logger,
) {
	logger.NoFields().Info("session manager: starting up")
	for {
		// Check for exit signal
		select {
		case <-done:
			logger.NoFields().Info("session manager: shutting down")
			close(output)
			return
		default:
		}
		// Establish a new session
		id, _, err := client.Session().CreateNoChecks(&config, nil)
		if err != nil {
			logger.WithError(err).Error("session manager: error creating Consul session")
			time.Sleep(time.Duration(*SessionRetrySeconds) * time.Second)
			continue
		}
		sessionLogger := logger.SubLogger(logrus.Fields{
			"session": id,
		})
		sessionLogger.NoFields().Info("session manager: new Consul session")
		select {
		case output <- id:
			// Maintain the session
			err = client.Session().RenewPeriodic(config.TTL, id, nil, done)
			if err != nil {
				sessionLogger.WithError(err).Error("session manager: lost session")
			} else {
				sessionLogger.NoFields().Info("session manager: released session")
			}
			output <- ""
		case <-done:
			// Don't bother reporting the new session if exiting
			_, _ = client.Session().Destroy(id, nil)
			sessionLogger.NoFields().Info("session manager: released session")
		}
	}
}

// WithSession executes the function f when there is an active session. When that session
// ends, f is signaled to exit. Once f finishes, a new execution will start when a new
// session begins.
//
// This function runs until the input stream of sessions is closed. Closing the "done"
// argument provides a shortcut to exit this function when also tearing down the session
// producer. In either case, any running f will be terminated before returning. A panic in
// f will also propagate upwards, causing the function to exit.
func WithSession(
	done <-chan struct{},
	sessions <-chan string,
	f func(done <-chan struct{}, session string),
) {
	// Start an asynchronous controller that will tell this goroutine when to execute the
	// user's function
	sentry := make(chan struct{})
	defer close(sentry)
	executions := make(chan execution)
	go withSessionController(sentry, executions, done, sessions)

	// Run the user's function when the controller says to
	for e := range executions {
		f(e.done, e.session)
		close(e.finished)
	}
}

type execution struct {
	finished chan<- struct{} // Signal from the executor that it has finished
	done     <-chan struct{} // Signal to the executor that it should abort
	session  string          // The session acquired
}

func withSessionController(
	sentry <-chan struct{}, // Will be closed when the executor exits
	executions chan<- execution, // Sends work to the executor
	done <-chan struct{}, // Outside signal to tear down all executions
	sessions <-chan string, // Sequence of session updates
) {
	var curSession string      // The current session identifier
	var fSession string        // The session f was last executed with
	var fRunning chan struct{} // If non-nil, f is running. F will close it when it stops
	var fDone chan struct{}    // If non-nil, f is running. Close to tell f to stop.

	// When exiting, stop all current and future work
	defer func() {
		if fDone != nil {
			close(fDone)
		}
	}()
	defer close(executions)

	for {
		if fDone != nil && (fRunning == nil || curSession != fSession) {
			// F needs to be cleaned up or stopped
			close(fDone)
			fDone = nil
		}
		if fRunning == nil && curSession != fSession && curSession != "" {
			// Start f if it isn't running and if the session has changed
			fSession = curSession
			fRunning = make(chan struct{})
			fDone = make(chan struct{})
			select {
			case executions <- execution{fRunning, fDone, fSession}:
			case <-sentry:
				return
			}
		}

		select {
		case s, ok := <-sessions: // Session changed
			if !ok {
				// Implicit request to exit, because there will be no more sessions
				return
			}
			curSession = s
		case <-fRunning: // F has exited
			fRunning = nil
		case <-sentry: // The executor exited
			return
		case <-done: // Explicit request to exit
			return
		}
	}
}
