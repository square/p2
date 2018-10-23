package launch

import (
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/docker/docker/api/types/strslice"
	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/util/size"
)

const (
	EntryPointEnvVar            = "ENTRY_POINT"
	HoistLaunchableType         = "hoist"
	OpenContainerLaunchableType = "opencontainer"
	DockerLaunchableType        = "docker"
)

type ArtifactName string

func (a ArtifactName) String() string { return string(a) }

type LaunchableID string

func (l LaunchableID) String() string { return string(l) }

type LaunchableVersionID string

func (l LaunchableVersionID) String() string { return string(l) }

type LaunchableVersion struct {
	// If present, overrides the artifact name to be used when discovering the artifact.
	// If absent, the name used for discovery defaults to the launchable ID.
	ArtifactOverride ArtifactName        `json:"artifact_name,omitempty" yaml:"artifact_name,omitempty"`
	ID               LaunchableVersionID `json:"id" yaml:"id"`
	Tags             map[string]string   `json:"tags,omitempty" yaml:"tags,omitempty"`
}

type LaunchableStanza struct {
	LaunchableType          string            `yaml:"launchable_type"`
	DigestLocation          string            `yaml:"digest_location,omitempty"`
	DigestSignatureLocation string            `yaml:"digest_signature_location,omitempty"`
	RestartTimeout          string            `yaml:"restart_timeout,omitempty"`
	CgroupConfig            cgroups.Config    `yaml:"cgroup,omitempty"`
	Env                     map[string]string `yaml:"env,omitempty"`
	RunAs                   string            `yaml:"run_as,omitempty"`

	// Indicates whether the processes started for the launchable should be
	// restarted when they terminate.  When unspecified, the default is
	// "always".
	RestartPolicy_ runit.RestartPolicy `yaml:"restart_policy,omitempty"`

	// NoHaltOnUpdate instructs the preparer to skip stopping the
	// launchable's processes when it is being updated. This is useful for
	// processes that are designed to be updated via binary overwrite and
	// SIGHUP for example.
	// NOTE: under certain circumstances the process still might be
	// stopped, for example if a host operator calls p2-shutdown
	// NOTE: P2 does not arrange for signals to your app to signify update,
	// that should be implemented by the pod itself via bin/post-activate
	NoHaltOnUpdate bool `yaml:"no_halt_on_update,omitempty"`

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

	// Image: the name of the container image to run. This only applies when the launchable
	// type is "docker"
	Image DockerImage `yaml:"image,omitempty"`

	// Entrypoint: only supported for docker launchables. This values specifies an entrypoint that will override the default entrypoint defined in the image
	EntryPoint strslice.StrSlice `yaml:"entrypoint,omitempty"`

	// PostStart: only supported for docker launchables. This value specifies what command to run after the container has started. This is equivalent to the enable script for hoist launchables
	PostStart PostStart `yaml:"postStart,omitempty"`

	// PreStop: only supported for docker launchables. This value specifies what command to run before the container is stopped. This is equivalent to the disable script for hoist launchables
	PreStop PreStop `yaml:"preStop,omitempty"`
}

// DockerImage contains launchable information specific to the "docker" launchable type.
type DockerImage struct {
	Name   string `yaml:"name"`
	SHA256 string `yaml:"sha256"`
}

// PostStart contains launchable information specific to the "docker" launchable type.
type PostStart struct {
	Exec Command `yaml:"exec"`
}

// PreStop contains launchable information specific to the "docker" launchable type.
type PreStop struct {
	Exec Command `yaml:"exec"`
}

// Command contains launchable information specific to the "docker" launchable type.
type Command struct {
	Command []string `yaml:"command"`
}

func (l LaunchableStanza) LaunchableVersion() (LaunchableVersionID, error) {
	if l.LaunchableType == HoistLaunchableType || l.LaunchableType == OpenContainerLaunchableType {
		if l.Version.ID != "" {
			return l.Version.ID, nil
		}

		return versionFromLocation(l.Location)
	}
	if l.LaunchableType == DockerLaunchableType {
		return LaunchableVersionID(l.Image.SHA256), nil
	}
	return "", util.Errorf("Unsupported launchable type %s", l.LaunchableType)
}

func (l LaunchableStanza) ImageDirectory() (string, error) {
	if l.LaunchableType == DockerLaunchableType {
		return strings.Split(l.Image.Name, "/")[2], nil
	}
	return "", util.Errorf("Unsupported launchable type %s", l.LaunchableType)
}

func (l LaunchableStanza) LaunchableImage() (string, error) {
	if l.LaunchableType == DockerLaunchableType {
		return fmt.Sprintf("%s@sha256:%s", l.Image.Name, l.Image.SHA256), nil
	}
	return "", util.Errorf("Unsupported launchable type %s", l.LaunchableType)
}

func (l LaunchableStanza) RestartPolicy() runit.RestartPolicy {
	if l.RestartPolicy_ == "" {
		return runit.DefaultRestartPolicy
	}

	return l.RestartPolicy_
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
	PostInstall() (string, error)

	// PostActive runs a Hoist-specific "post-activate" script in the launchable.
	PostActivate() (string, error)
	// Launch begins execution.
	Launch(serviceBuilder *runit.ServiceBuilder, sv runit.SV) error
	// Disable allows a launchable to stop work and do cleanup prior to Stop
	Disable() error
	// Stop stops execution.
	Stop(serviceBuilder *runit.ServiceBuilder, sv runit.SV, force bool) error
	// MakeCurrent adjusts a "current" symlink for this launchable name to point to this
	// launchable's version.
	MakeCurrent() error
	// Prune performs necessary cleanup for a particular launchable's resources, should it
	// be necessary. The provided argument is guidance for how many bytes on disk a particular
	// launchable should consume
	Prune(size.ByteCount) error

	// RestartTimeout returns the RestartTimeout
	GetRestartTimeout() time.Duration

	// Env vars that will be exported to the launchable for its launch script and other hooks.
	EnvVars() map[string]string

	RestartPolicy() runit.RestartPolicy
}

// Executable describes a command and its arguments that should be executed to start a
// service running.
type Executable struct {
	ServiceName   string // e.g. "bin__launch"
	RelativePath  string // relative path to executable within launchable, e.g. "bin/launch"
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
