package launch

import (
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"

	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/util/size"
)

type LaunchableID string

func (l LaunchableID) String() string { return string(l) }

type LaunchableVersionID string

func (l LaunchableVersionID) String() string { return string(l) }

type LaunchableVersion struct {
	ID   LaunchableVersionID `yaml:"id"`
	Tags map[string]string   `yaml:"tags,omitempty"`
}

type LaunchableStanza struct {
	LaunchableType          string            `yaml:"launchable_type"`
	LaunchableId            LaunchableID      `yaml:"launchable_id"`
	DigestLocation          string            `yaml:"digest_location,omitempty"`
	DigestSignatureLocation string            `yaml:"digest_signature_location,omitempty"`
	RestartTimeout          string            `yaml:"restart_timeout,omitempty"`
	CgroupConfig            cgroups.Config    `yaml:"cgroup,omitempty"`
	Env                     map[string]string `yaml:"env,omitempty"`

	// Specifies which files or directories (relative to launchable root)
	// should be launched under runit. Only launchables of type "hoist"
	// make use of this field, and if empty, a default of ["bin/launch"]
	// is used
	EntryPoints []string `yaml:"entry_points,omitempty"`

	// The URL from which the launchable can be downloaded. May not be used
	// in conjunction with Version
	Location string `yaml:"location,omitempty"`

	// An alternative to using Location to inform artifact downloading. Version information
	// can be used to query a configured artifact registry which will provide the artifact
	// URL. Version may not be used in conjunction with Location
	Version LaunchableVersion `yaml:"version,omitempty"`
}

func (l LaunchableStanza) LaunchableVersion() (LaunchableVersionID, error) {
	if l.Version.ID != "" {
		return l.Version.ID, nil
	}

	return versionFromLocation(l.Location)
}

// Uses the assumption that all locations have a Path component ending in
// /<launchable_id>_<version>.tar.gz, which is intended to be phased out in
// favor of explicit launchable versions specified in pod manifests.
// The version expected to be a 40 character hexadecimal string with an
// optional hexadecimal suffix after a hyphen

var locationBaseRegex = regexp.MustCompile(`^[a-z0-9-_]+_([a-f0-9]{40}(\-[a-z0-9]+)?)\.tar\.gz$`)

func versionFromLocation(location string) (LaunchableVersionID, error) {
	filename := path.Base(location)
	parts := locationBaseRegex.FindStringSubmatch(filename)
	if parts == nil {
		return "", util.Errorf("Malformed filename in URL: %s", filename)
	}

	return LaunchableVersionID(parts[1]), nil
}

const DefaultAllowableDiskUsage = 10 * size.Gibibyte

type DisableError struct{ Inner error }

func (e DisableError) Error() string { return e.Inner.Error() }

type EnableError struct{ Inner error }

func (e EnableError) Error() string { return e.Inner.Error() }

type StartError struct{ Inner error }

func (e StartError) Error() string { return e.Inner.Error() }

type StopError struct{ Inner error }

func (e StopError) Error() string { return e.Inner.Error() }

// Launchable describes a type of app that can be downloaded and launched.
type Launchable interface {
	// Type returns a text description of the type of launchable.
	Type() string
	// ID returns a (pod-wise) unique ID for this launchable.
	ID() LaunchableID
	// ServiceID returns a (host-wise) unique ID for this launchable.
	// Unlike ID(), ServiceID() must be unique for all instances of a launchable
	// on a single host, even if are multiple pods have the same launchable ID.
	// This is because runit requires service names to be unique.
	// In practice this usually means this will return some concatenation of the
	// pod ID and the launchable ID.
	ServiceID() string
	// InstallDir is the directory where this launchable is or will be placed.
	InstallDir() string
	// EnvDir is the directory in which launchable environment variables
	// will be expressed as files
	EnvDir() string
	// Executables gets a list of the commands that are part of this launchable.
	Executables(serviceBuilder *runit.ServiceBuilder) ([]Executable, error)
	// Installed returns true if this launchable is already installed.
	Installed() bool
	// Executes any necessary post-install steps to ready the launchable for launch
	PostInstall() error

	// PostActive runs a Hoist-specific "post-activate" script in the launchable.
	PostActivate() (string, error)
	// Launch begins execution.
	Launch(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error
	// Halt stops execution.
	Halt(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error
	// MakeCurrent adjusts a "current" symlink for this launchable name to point to this
	// launchable's version.
	MakeCurrent() error
	// Prune performs necessary cleanup for a particular launchable's resources, should it
	// be necessary. The provided argument is guidance for how many bytes on disk a particular
	// launchable should consume
	Prune(size.ByteCount) error

	// Env vars that will be exported to the launchable for its launch script and other hooks.
	EnvVars() map[string]string
}

// Executable describes a command and its arguments that should be executed to start a
// service running.
type Executable struct {
	Service       runit.Service
	LogAgent      runit.Service
	Exec          []string
	RestartPolicy runit.RestartPolicy
}

func (e Executable) WriteExecutor(writer io.Writer) error {
	_, err := io.WriteString(
		writer, fmt.Sprintf(`#!/bin/sh
exec %s
`, strings.Join(e.Exec, " ")))
	return err
}
