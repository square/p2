package podprocess

import (
	"os"

	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"

	"github.com/sirupsen/logrus"
)

type EnvironmentExtractor struct {
	DatabasePath string
	Logger       logging.Logger
}

type RetryableError struct {
	Inner error
}

func (r RetryableError) Error() string {
	return r.Inner.Error()
}

func IsRetryable(err error) bool {
	_, ok := err.(RetryableError)
	return ok
}

// Write exit code and exit status along with some information pulled from the current
// environment into the configured sqlite database. The environment extractor is hooked
// into runits ./finish file (http://smarden.org/runit/runsv.8.html)
func (e EnvironmentExtractor) WriteFinish(exitCode int, exitStatus int) error {
	finish, err := e.constructFinishFromEnvironment(exitCode, exitStatus)
	if err != nil {
		return err
	}

	logger := e.Logger.SubLogger(logrus.Fields{
		"component": "finish_environment_extractor",
	})
	finishService, err := NewSQLiteFinishService(e.DatabasePath, logger)
	if err != nil {
		return err
	}

	err = finishService.Insert(finish)
	if err != nil {
		return RetryableError{
			Inner: err,
		}
	}

	return nil
}

func (e EnvironmentExtractor) constructFinishFromEnvironment(exitCode int, exitStatus int) (FinishOutput, error) {
	podID := os.Getenv(pods.PodIDEnvVar)
	if podID == "" {
		return FinishOutput{}, util.Errorf("No %s env var set", pods.PodIDEnvVar)
	}

	launchableID := os.Getenv(pods.LaunchableIDEnvVar)
	if launchableID == "" {
		return FinishOutput{}, util.Errorf("No %s env var set", pods.LaunchableIDEnvVar)
	}

	entryPoint := os.Getenv(launch.EntryPointEnvVar)
	if entryPoint == "" {
		return FinishOutput{}, util.Errorf("No %s env var set", launch.EntryPointEnvVar)
	}

	// It's okay if this one is missing, most pods are "legacy" pods that have a blank unique key
	podUniqueKey := os.Getenv(pods.PodUniqueKeyEnvVar)

	return FinishOutput{
		PodID:        types.PodID(podID),
		LaunchableID: launch.LaunchableID(launchableID),
		EntryPoint:   entryPoint,
		PodUniqueKey: types.PodUniqueKey(podUniqueKey),
		ExitCode:     exitCode,
		ExitStatus:   exitStatus,
	}, nil
}
