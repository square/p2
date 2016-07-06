package hooks

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/manifest"
	"github.com/square/p2/pkg/pods"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"
)

// Populates the given directory with executor scripts for each launch script of
// the given pod, which must be installed. Any orphaned executor scripts (from a
// past install, but no longer present in this pod) will be cleaned out.
func InstallHookScripts(dir string, hookPod *pods.Pod, manifest manifest.Manifest, logger logging.Logger) error {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	// TODO: globbing based on the structure of the name is gross, each hook pod
	// should have its own dir of scripts and running hooks should iterate over
	// the directories
	rmPattern := filepath.Join(dir, fmt.Sprintf("%s__*", hookPod.Id))
	matches, err := filepath.Glob(rmPattern)
	if err != nil {
		// error return from filepath.Glob is guaranteed to be ErrBadPattern
		return util.Errorf("error while removing old hook scripts: pattern %q is malformed", rmPattern)
	}
	for _, match := range matches {
		err = os.Remove(match)
		if err != nil {
			logger.WithErrorAndFields(err, logrus.Fields{"script_path": match}).Errorln("Could not remove old hook script")
		}
	}

	launchables, err := hookPod.Launchables(manifest)
	if err != nil {
		return err
	}
	for _, launchable := range launchables {
		if launchable.Type() != "hoist" {
			logger.WithFields(logrus.Fields{
				"id":   launchable.ServiceID(),
				"type": launchable.Type(),
			}).Errorln("hook disabled: unsupported launchable type")
			continue
		}
		executables, err := launchable.Executables(runit.DefaultBuilder)
		if err != nil {
			return err
		}

		for _, executable := range executables {
			// Write a script to the event directory that executes the pod's executables
			// with the correct environment for that pod.
			scriptPath := filepath.Join(dir, executable.Service.Name)
			file, err := os.OpenFile(scriptPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0744)
			defer file.Close()
			if err != nil {
				logger.WithErrorAndFields(err, logrus.Fields{"script_path": scriptPath}).Errorln("Could not open new hook script")
			}
			err = executable.WriteExecutor(file)
			if err != nil {
				logger.WithErrorAndFields(err, logrus.Fields{"script_path": scriptPath}).Errorln("Could not write new hook script")
			}
		}
		// for convenience as we do with regular launchables, make these ones
		// current under the launchable directory
		err = launchable.MakeCurrent()
		if err != nil {
			logger.WithErrorAndFields(err, logrus.Fields{"launchable_id": launchable.ServiceID()}).
				Errorln("Could not set hook launchable to current")
		}
	}
	return nil
}
