/*
This package provides a server listening on a unix socket for the exit status
of pods. The status of each pod is written back to the consul datastore. This
is useful for allowing external systems to examine the success or failure of a
pod.
*/
package podprocess

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"os"
	"time"

	"github.com/square/p2/pkg/kp/statusstore/podstatus"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/Sirupsen/logrus"
)

type ReporterConfig struct {
	// Path to the socket that should receive finish information
	ProcessResultSocketPath string `yaml:"process_result_socket_path"`

	// Path to the executable that constructs finish information based on process environment
	EnvironmentExtractorPath string `yaml:"environment_extractor_path"`
}

type Reporter struct {
	sockPath                 string
	environmentExtractorPath string

	logger         logging.Logger
	podStatusStore podstatus.Store
}

// Returns a (reporter, true) if configuration is sufficient to create a reporter. Returns (nil, false) if configuration
// does not require a reporter.
func New(config ReporterConfig, logger logging.Logger, podStatusStore podstatus.Store) (*Reporter, bool) {
	if config.ProcessResultSocketPath == "" {
		logger.Infoln("process_result_socket_path not configured, process exit status will not be captured")
		return nil, false
	}

	// TODO: do more validation here?
	if config.EnvironmentExtractorPath == "" {
		logger.Infoln("environment_extractor_path not configured, process exit status will not be captured")
		return nil, false
	}

	return &Reporter{
		sockPath:                 config.ProcessResultSocketPath,
		environmentExtractorPath: config.EnvironmentExtractorPath,
		logger:         logger,
		podStatusStore: podStatusStore,
	}, true
}

func (r *Reporter) Run(quitCh <-chan struct{}) error {
	return r.run(quitCh, 0622)
}

// This function is broken out so that tests can put more permissive
// permissions
func (r *Reporter) run(quitCh <-chan struct{}, socketPerms os.FileMode) error {
	if err := os.Remove(r.sockPath); err != nil && !os.IsNotExist(err) {
		return util.Errorf("Could not remove the socket file: %v", err)
	}

	server, err := net.Listen("unix", r.sockPath)
	if err != nil {
		return util.Errorf("could not bind sock: %v", err)
	}

	r.logger.Infof("Listening for process exit events on %s", r.sockPath)
	if err := os.Chmod(r.sockPath, socketPerms); err != nil {
		server.Close()
		return util.Errorf("could not set FileMode for socket at %s: %v", r.sockPath, err)
	}

	go func() {
		<-quitCh
		server.Close()
	}()

	for {
		conn, err := server.Accept()
		if err != nil {
			select {
			case <-quitCh:
				// We expect an error when server.Close() is called, it's time to shut down
				r.logger.Infoln("Process event reporter shutting down")
				return nil
			default:
			}
			r.logger.Errorf("Error during Accept(): %v", err)
			continue
		}

		go func(conn net.Conn) {
			defer conn.Close()

			var buf bytes.Buffer
			io.Copy(&buf, conn)
			r.publishPodStatus(r.decodeBase64(buf.Bytes()))
		}(conn)
	}
}

type FinishOutput struct {
	PodID        types.PodID         `json:"pod_id"`
	LaunchableID launch.LaunchableID `json:"launchable_id"`
	EntryPoint   string              `json:"entry_point"`
	PodUniqueKey types.PodUniqueKey  `json:"pod_unique_key"`

	// These two model the arguments given to the ./finish script under runit:
	// (http://smarden.org/runit/runsv.8.html)
	ExitCode   int `json:"exit_code"`
	ExitStatus int `json:"exit_status"`
}

func (r *Reporter) decodeBase64(payload []byte) []byte {
	buf := make([]byte, base64.StdEncoding.DecodedLen(len(payload)))
	n, err := base64.StdEncoding.Decode(buf, payload)
	if n == 0 {
		r.logger.Errorln("Got empty payload, ignoring.")
		return nil
	}
	if err != nil {
		r.logger.WithError(err).Errorln("Unable to decode Base64 payload, recovering by skipping this message")
		return nil
	}
	buf = buf[0:n]
	return buf
}

func (r *Reporter) publishPodStatus(finishBytes []byte) {
	if finishBytes == nil {
		// There was a problem with base64 decoding, error already logged
		return
	}

	var finishOutput FinishOutput
	err := json.Unmarshal(finishBytes, &finishOutput)
	if err != nil {
		r.logger.WithError(err).Errorf("Failed to unmarshal '%s'", string(finishBytes))
		return
	}

	subLogger := r.logger.SubLogger(logrus.Fields{
		"pod_id":         finishOutput.PodID,
		"launchable_id":  finishOutput.LaunchableID,
		"entry_point":    finishOutput.EntryPoint,
		"pod_unique_key": finishOutput.PodUniqueKey,
		"exit_code":      finishOutput.ExitCode,
		"exit_status":    finishOutput.ExitStatus,
	})
	subLogger.Infoln("Received process exit information")

	if finishOutput.PodUniqueKey == "" {
		// Status is only written to consul for uuid pods
		return
	}

	err = r.podStatusStore.SetLastExit(finishOutput.PodUniqueKey, finishOutput.LaunchableID, finishOutput.EntryPoint, podstatus.ExitStatus{
		ExitTime:   time.Now(),
		ExitCode:   finishOutput.ExitCode,
		ExitStatus: finishOutput.ExitStatus,
	})
	if err != nil {
		subLogger.WithError(err).Errorln("Failed to record status")
		return
	}

	subLogger.Infoln("Successfully recorded status")
}
