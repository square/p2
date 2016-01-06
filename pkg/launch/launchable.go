package launch

import (
	"fmt"
	"io"
	"strings"

	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/uri"
	"github.com/square/p2/pkg/util/size"
)

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
	// Fetcher returns a uri.Fetcher that is capable of fetching the launchable files.
	Fetcher() uri.Fetcher

	// Install acquires the launchable and makes it ready to be launched.
	Install() error
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
}

// Executable describes a command and its arguments that should be executed to start a
// service running.
type Executable struct {
	Service       runit.Service
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
