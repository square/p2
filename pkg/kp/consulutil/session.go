package consulutil

import (
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"

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
	client *api.Client,
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
			client.Session().Destroy(id, nil)
			sessionLogger.NoFields().Info("session manager: released session")
		}
	}
}
