package main

import (
	"flag"
	. "github.com/platypus-platform/pp/pkg/logging"
	"github.com/platypus-platform/pp/pkg/store"
	"gopkg.in/yaml.v1"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
)

type SetliveConfig struct {
	Sbpath           string
	RunitStagingPath string
	RunitPath        string
}

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		Fatal(err.Error())
		os.Exit(1)
	}

	var config SetliveConfig

	flag.StringVar(&config.Sbpath, "config",
		"fake/servicebuilder.d", "config directory for servicebuilder")
	flag.StringVar(&config.RunitStagingPath, "runit-stage",
		"fake/service-stage", "runit staging directory")
	flag.StringVar(&config.RunitPath, "runit",
		"fake/service", "runit directory")
	flag.Parse()

	err = pp.PollIntent(hostname, func(intent pp.IntentNode) {
		for _, app := range intent.Apps {
			setLive(config, app)
		}
	})

	if err != nil {
		Fatal(err.Error())
		os.Exit(1)
	}
}

func setLive(config SetliveConfig, app pp.IntentApp) {
	version := app.ActiveVersion()

	if version == "" {
		// TODO: Ensure stopped
		// TODO: Ensure deregistered
		Info("%s: No active version, skipping", app.Name)
		return
	}

	install := path.Join(app.Basedir, "installs", app.Name+"_"+version)
	current := path.Join(app.Basedir, "current")

	if _, err := os.Stat(install); os.IsNotExist(err) {
		Info("%s: %s not prepared, skipping", app.Name, app.ActiveVersion())
		return
	}

	client, err := pp.NewRealityClient()
	if err != nil {
		Fatal("%s: could not connect to reality", app.Name)
		return
	}

	currentInstall, err := os.Readlink(current)
	if err != nil {
		Warn("%s: error reading link: %s", app.Name, err)
	}

	healthy, err := client.LocalHealthy(app.Name)
	if err != nil {
		Fatal("%s: could not query local health: %s", app.Name, err)
		return
	} else if currentInstall == install && healthy {
		Info("%s: already healthy and current, noop", app.Name)
		return
	}

	Info("%s: commencing shutdown", app.Name)

	if healthy {
		Info("%s: acquiring lease", app.Name)
		lease := client.AcquireShutdownLease(app.Name, app.MinNodes)
		if lease != nil {
			func() {
				defer client.ReleaseLease(lease)
				Info("%s: acquired lease", app.Name)

				cmd := exec.Command("sv", "stop", path.Join(config.RunitPath, app.Name))
				if err := cmd.Run(); err != nil {
					Fatal("%s: Could not stop: %s", app.Name, err)
					return
				}

				// Do not release lease until unhealthiness has propagated.
				Info("%s: waiting to become unhealthy", app.Name)
				if err := client.WaitForLocalUnhealthy(app.Name); err != nil {
					Fatal("%s: could not query local health: %s", app.Name, err)
					return
				}
			}()
		} else {
			Info("%s: did not acquire lease, will try again later", app.Name)
			return
		}
	} else {
		Info("%s: unhealthy, not acquiring lease", app.Name)
		// No need to coordinate for unhealthy processes, just ensure they are
		// stopped.
		cmd := exec.Command("sv", "stop", path.Join(config.RunitPath, app.Name))
		if err := cmd.Run(); err != nil {
			Fatal("%s: Could not stop: %s", app.Name, err)
			return
		}
	}

	// Ensure servicebuilder files up-to-date
	// TODO: How to provide all our custom options here? Template?
	Info("%s: Configuring service builder", app.Name)
	if err := configureServiceBuilder(config, app); err != nil {
		Fatal("%s: Could not configure servicebuilder: %s", app.Name, err)
		return
	}

	Info("%s: Symlinking", app.Name)

	if _, err := os.Stat(current); err == nil {
		if err := os.Remove(current); err != nil {
			Fatal("%s: Could not remove current symlink: %s", app.Name, err)
			return
		}
	}

	if err := os.Symlink(install, current); err != nil {
		Fatal("%s: Could not symlink current: %s", app.Name, err)
		return
	}

	// TODO: Allow bin/check in artifacts
	healthCheck := "grep -q run " +
		path.Join(config.RunitPath, app.Name, "supervise/stat")

	Info("%s: Registering service", app.Name)
	client.RegisterService(app.Name, healthCheck)

	Info("%s: Starting", app.Name)

	cmd := exec.Command("sv", "start", path.Join(config.RunitPath, app.Name))
	if err := cmd.Run(); err != nil {
		Fatal("%s: Could not start: %s", app.Name, err)
		return
	}

	// Wait for healthy otherwise we may loop again and try to upgrade again
	// while service is still starting up.
	if err := client.WaitForLocalHealthy(app.Name); err != nil {
		Fatal("%s: $rror waiting for healthy: %s", app.Name, err)
		return
	}
}

func configureServiceBuilder(config SetliveConfig, app pp.IntentApp) error {
	sb := map[string]interface{}{}
	sb[app.Name] = map[string][]string{
		"run": []string{
			path.Join(app.Basedir, "current/bin/launch"), // TODO Support directory
		},
	}

	data, err := yaml.Marshal(&sb)
	if err != nil {
		return err
	}

	sbFile := path.Join(config.Sbpath, app.Name+".yaml")
	err = ioutil.WriteFile(sbFile, data, 0644)
	if err != nil {
		return err
	}

	sbCmd := exec.Command("servicebuilder",
		"-c", config.Sbpath,
		"-d", config.RunitPath,
		"-s", config.RunitStagingPath,
	)
	if err := sbCmd.Run(); err != nil {
		return err
	}
	return nil
}
