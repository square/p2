// Package runit provides a programmatic way to interact with runit and
// servicebuilder (https://github.com/square/prodeng/blob/master/servicebuilder/README.md).
// You can use this package to make it easy to write new sb configs and exec sb.
//
// Example usage:
//
// import "github.com/square/p2/pkg/runit"
//
// template := runit.NewSBTemplate("exampleapp")
// template.AddEntry("exampleapp_web", []string{"python", "-m", "SimpleHTTPServer"})
// template.AddEntry("exampleapp_redis", []string{"redis", "-p", "6603"})
// outPath, err := runit.DefaultBuilder.Write(template) // write the new config
// if err != nil {
//    return nil, err
// }
// err = runit.DefaultBuilder.Rebuild() // rebuild
//
package runit

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/square/p2/pkg/p2exec"
	"github.com/square/p2/pkg/util"

	"gopkg.in/yaml.v2"
)

// These are in the runit package to avoid import cycles.
type RestartPolicy string
type Exec []string

const (
	RestartPolicyAlways RestartPolicy = "always"
	RestartPolicyNever  RestartPolicy = "never"

	DefaultRestartPolicy = RestartPolicyAlways

	DOWN_FILE_NAME = "down"
)

// To maintain compatibility with Ruby1.8's YAML serializer, a document separator with a
// trailing space must be used.
const yamlSeparator = "--- "

func DefaultLogExec() []string {
	args := p2exec.P2ExecArgs{
		User:    "nobody",
		Command: []string{"svlogd", "-tt", "./main"},
	}.CommandLine()
	return append([]string{p2exec.DefaultP2Exec}, args...)
}

type ServiceTemplate struct {
	Run      []string `yaml:"run"`
	Log      []string `yaml:"log,omitempty"`
	Finish   []string `yaml:"finish,omitempty"`
	Sleep    *int     `yaml:"sleep,omitempty"`
	LogSleep *int     `yaml:"logsleep,omitempty"`

	// Determines whether the service should be restarted by runit after it
	// exits. This isn't actually written to the servicebuilder file but
	// determines how the stage directory is written and the service
	// invoked
	// TODO: write this to the servicebuilder file and use it to determine
	// how the service should be started
	RestartPolicy RestartPolicy `yaml:"-"`
}

func (s ServiceTemplate) runScript() ([]byte, error) {
	if len(s.Run) == 0 {
		return nil, util.Errorf("empty run script")
	}

	args, err := yaml.Marshal(s.Run)
	if err != nil {
		return nil, err
	}

	// default sleep to reduce spinning on a broken run script
	sleep := 2
	if s.Sleep != nil && *s.Sleep >= 0 {
		sleep = *s.Sleep
	}

	ret := fmt.Sprintf(`#!/usr/bin/ruby
$stderr.reopen(STDOUT)
require 'yaml'
sleep %d
exec "chpst", "-P", "--", *YAML.load(DATA.read)
sleep 2
__END__
%s
%s
`, sleep, yamlSeparator, args)
	return []byte(ret), nil
}

func (s ServiceTemplate) logScript() ([]byte, error) {
	sleep := 2
	if s.LogSleep != nil && *s.LogSleep >= 0 {
		sleep = *s.LogSleep
	}

	log := s.Log

	if len(log) == 0 {
		// use a default log script that makes a logdir, chowns it and execs
		// svlogd into it
		log = DefaultLogExec()
	}

	args, err := yaml.Marshal(log)
	if err != nil {
		return nil, err
	}

	ret := fmt.Sprintf(`#!/usr/bin/ruby
require 'yaml'
sleep %d
exec "chpst", "-P", "--", *YAML.load(DATA.read)
sleep 2
__END__
%s
%s
`, sleep, yamlSeparator, args)
	return []byte(ret), nil
}

func (s ServiceTemplate) finishScript() ([]byte, error) {
	finish_exec := s.Finish
	if len(finish_exec) == 0 {
		finish_exec = []string{"/bin/true", "# finish not implemented"}
	}
	finishScript := fmt.Sprintf(`#!/bin/bash
%s
`, strings.Join(finish_exec, " "))

	return []byte(finishScript), nil
}

type ServiceBuilder struct {
	ConfigRoot  string // directory to generate YAML files
	StagingRoot string // directory to place staged runit services
	RunitRoot   string // directory of runsvdir
	Bin         string

	// testingNoChown should be set during unit tests to prevent the chown() operation when
	// staging a service. Unit tests run as normal users, not root, so the chown() will fail
	// without allowing for any tests to run.
	testingNoChown bool
}

var DefaultBuilder = &ServiceBuilder{
	ConfigRoot:  "/etc/servicebuilder.d",
	StagingRoot: "/var/service-stage",
	RunitRoot:   "/var/service",
	Bin:         "/usr/bin/servicebuilder",
}

// DefaultChpst is the path to the default chpst binary. Specified as a var so you
// can override at build time.
var DefaultChpst = "/usr/bin/chpst"

// write a servicebuilder yaml file
// in addition to backwards compat with the real servicebuilder, this file
// stores the truth of what services are "supposed" to exist right now, which
// is needed for pruning unused services later
func (s *ServiceBuilder) write(path string, templates map[string]ServiceTemplate) error {
	if len(templates) == 0 {
		// only write non-empty servicebuilder files
		if _, err := os.Stat(path); err == nil {
			if err = os.Remove(path); err != nil {
				return util.Errorf("Could not remove old servicebuilder template when new path should have no services: %s", err)
			}
		}
		return nil
	}

	text, err := yaml.Marshal(templates)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, text, 0644)
}

// convert the servicebuilder yaml file into a runit service directory
func (s *ServiceBuilder) stage(templates map[string]ServiceTemplate) error {
	nobody, err := user.Lookup("nobody")
	if err != nil {
		return err
	}
	nobodyUid, err := strconv.ParseInt(nobody.Uid, 10, 64)
	if err != nil {
		return err
	}
	nobodyGid, err := strconv.ParseInt(nobody.Gid, 10, 64)
	if err != nil {
		return err
	}

	for serviceName, template := range templates {
		stageDir := filepath.Join(s.StagingRoot, serviceName)
		// create the default log directory
		logDir := filepath.Join(stageDir, "log")
		err := os.MkdirAll(logDir, 0755)
		if err != nil {
			return err
		}

		// create a place for the logs to go
		logMainDir := filepath.Join(logDir, "main")
		err = os.Mkdir(logMainDir, 0755)
		if err == nil {
			if !s.testingNoChown {
				err = os.Chown(logMainDir, int(nobodyUid), int(nobodyGid))
				if err != nil {
					return err
				}
			}
		} else if !os.IsExist(err) {
			return err
		}

		runScript, err := template.runScript()
		if err != nil {
			return err
		}
		if _, err := util.WriteIfChanged(filepath.Join(stageDir, "run"), runScript, 0755); err != nil {
			return err
		}

		logScript, err := template.logScript()
		if err != nil {
			return err
		}
		if _, err := util.WriteIfChanged(filepath.Join(stageDir, "log", "run"), logScript, 0755); err != nil {
			return err
		}

		finishScript, err := template.finishScript()
		if err != nil {
			return err
		}
		if _, err := util.WriteIfChanged(filepath.Join(stageDir, "finish"), finishScript, 0755); err != nil {
			return err
		}

		// If a "down" file is not present, runit will restart the process
		// whenever it finishes. Prevent that if the requested restart policy
		// is not RestartAlways
		downPath := filepath.Join(stageDir, DOWN_FILE_NAME)
		if template.RestartPolicy != RestartPolicyAlways {
			file, err := os.Create(downPath)
			if err != nil {
				return err
			}
			_ = file.Close()
		} else {
			// There could be a down file lying around from a
			// previous installation with a different restart
			// policy, check for it and remove it if present
			_, err := os.Stat(downPath)
			if err == nil {
				err := os.Remove(downPath)
				if err != nil {
					return util.Errorf("Unable to remove down file: %s", err)
				}
			} else if !os.IsNotExist(err) {
				return util.Errorf("Error checking for down file: %s", err)
			}
		}
	}
	return nil
}

// symlink the runit service directory into the actual directory being monitored
// by runsvdir
// runsvdir will automatically start a service for each new directory (unless a
// down file exists)
func (s *ServiceBuilder) activate(templates map[string]ServiceTemplate) error {
	for serviceName := range templates {
		linkPath := filepath.Join(s.RunitRoot, serviceName)
		stageDir := filepath.Join(s.StagingRoot, serviceName)

		info, err := os.Lstat(linkPath)
		if err == nil {
			// if it exists, make sure it is actually a symlink
			if info.Mode()&os.ModeSymlink == 0 {
				return util.Errorf("%s is not a symlink", linkPath)
			}

			// and that it points to the right place
			target, err := os.Readlink(linkPath)
			if err != nil {
				return err
			}
			if target != stageDir {
				return util.Errorf("%s is a symlink to %s (expected %s)", linkPath, target, stageDir)
			}
		} else if os.IsNotExist(err) {
			if err = os.Symlink(stageDir, linkPath); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	return nil
}

// public API to write, stage and activate a servicebuilder template
func (s *ServiceBuilder) Activate(name string, templates map[string]ServiceTemplate) error {
	if err := s.write(filepath.Join(s.ConfigRoot, name+".yaml"), templates); err != nil {
		return err
	}
	if err := s.stage(templates); err != nil {
		return err
	}

	return s.activate(templates)
}

// using the servicebuilder yaml files, find any extraneous runit services and
// remove them
// runsvdir automatically stops services that no longer exist, explicit stop is
// not required
func (s *ServiceBuilder) Prune() error {
	configs, err := s.loadConfigDir()
	if err != nil {
		return err
	}

	links, err := ioutil.ReadDir(s.RunitRoot)
	if err != nil {
		return err
	}
	for _, link := range links {
		if _, exists := configs[link.Name()]; !exists {
			if err = os.Remove(filepath.Join(s.RunitRoot, link.Name())); err != nil {
				return err
			}
		}
	}

	stages, err := ioutil.ReadDir(s.StagingRoot)
	if err != nil {
		return err
	}
	for _, stage := range stages {
		if _, exists := configs[stage.Name()]; !exists {
			if err = os.RemoveAll(filepath.Join(s.StagingRoot, stage.Name())); err != nil {
				return err
			}
		}
	}

	return nil
}

// loadConfigDir reads all the files in the ConfigRoot and converts them into a
// single map of servicetemplates.
func (s *ServiceBuilder) loadConfigDir() (map[string]ServiceTemplate, error) {
	ret := make(map[string]ServiceTemplate)

	entries, err := ioutil.ReadDir(s.ConfigRoot)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		entryContents, err := ioutil.ReadFile(filepath.Join(s.ConfigRoot, entry.Name()))
		if err != nil {
			return nil, err
		}

		entryTemplate := make(map[string]ServiceTemplate)
		err = yaml.Unmarshal(entryContents, entryTemplate)
		if err != nil {
			return nil, err
		}

		for name, template := range entryTemplate {
			if _, exists := ret[name]; exists {
				return nil, util.Errorf("service with name %s was defined twice (from %s)", name, entry.Name())
			}
			ret[name] = template
		}
	}

	return ret, nil
}
