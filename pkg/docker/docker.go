// package docker implements a docker launchable type
package docker

import (
	"context"
	"path/filepath"
	"time"

	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/util/size"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	dockerclient "github.com/docker/docker/client"

	// add this import explicitly for unix
	_ "github.com/opencontainers/runc/libcontainer/system"
)

var _ launch.Launchable = &Launchable{}

type Launchable struct {
	// LaunchableID is the ID of the launchable within the pod manifest, not to be
	// confused with the container ID or the container name we'll use to
	// identify the container with docker.
	LaunchableID launch.LaunchableID

	// The name of the docker image to run, as if passed to the docker
	// command line
	Image string

	// RunAs is the name of the user the container should run as inside the container
	RunAs string

	// RootDir is where P2 writes files related to this container, not to be
	// confused with the container filesystem's location on disk which is managed
	// by dockerd
	RootDir string

	// RestartTimeout is poorly named in the context of docker containers, since
	// we never restart a container, only stop them and start them. In this case
	// it's the amount of time we ask docker to give a container to stop after
	// SIGTERM is sent before SIGKILL
	RestartTimeout time.Duration

	// Unique name for log messages to uniquely identify the pod and container
	ServiceID_ string

	// DockerClient is the client used to start and stop containers
	DockerClient *dockerclient.Client

	// EntryPoints are the commands to run in the container
	EntryPoints []string

	// ParentCgroupID is the name of the pod-wide cgroup. Docker containers will
	// be configured with cgroups inheriting from this one.
	ParentCgroupID string

	// CPUQuota is the number of CPUs the cgroup is allowed to use. Technically, this
	// measurement is in "millions of microseconds per second", so it should be multiplied
	// by a million before being passed to docker
	CPUQuota int

	// CgroupMemorySize is the max allowed memory docker should set for the cgroup for
	// the container
	CgroupMemorySize size.ByteCount

	// RestartPolicy_ determines what behavior docker should have when the pod exits.
	// Typically it will indicate that docker should always restart the container
	// for long-running services (e.g. servers)
	RestartPolicy_ runit.RestartPolicy
}

func (l *Launchable) Disable() error {
	// TODO: implement
	return nil
}

func (l *Launchable) EnvDir() string {
	return filepath.Join(l.RootDir, "env")
}

func (l *Launchable) EnvVars() map[string]string {
	// TODO: implement
	return nil
}

func (l *Launchable) Executables(_ *runit.ServiceBuilder) ([]launch.Executable, error) {
	// We don't make use of runit for docker launchables, so there are no
	// executables to supply. Instead we'll just send "docker run" and
	// "docker stop" commands to dockerd.
	return nil, nil
}

func (l *Launchable) GetRestartTimeout() time.Duration {
	return l.RestartTimeout
}

func (l *Launchable) ID() launch.LaunchableID {
	return l.LaunchableID
}

func (l *Launchable) InstallDir() string {
	// There is no install directory for a docker container
	return ""
}

func (l *Launchable) Installed() bool {
	// docker pull is idempotent and cheap, so don't worry about this
	return false
}

func (l *Launchable) Launch(_ *runit.ServiceBuilder, _ runit.SV) error {
	if l.DockerClient == nil {
		return util.Errorf("docker client was not initialized, can't launch docker launchable")
	}

	ctx := context.TODO()
	containerConfig := &container.Config{
		User:  l.RunAs, // TODO: this will break most of the time since the user doesn't exist in the container
		Image: l.Image,
	}

	restartPolicy := "always"
	switch l.RestartPolicy_ {
	case runit.RestartPolicyAlways:
		// keep it the way it is
	case runit.RestartPolicyNever:
		restartPolicy = "no"
	default:
		return util.Errorf("invalid restart policy: %s", l.RestartPolicy_)
	}
	hostConfig := &container.HostConfig{
		Binds:       []string{}, // TODO: probably bind in TMPDIR and LOGDIR
		NetworkMode: "host",
		RestartPolicy: container.RestartPolicy{
			Name:              restartPolicy,
			MaximumRetryCount: 0, // This is ignored unless Name is "on-failure"
		},
		AutoRemove: false,
		Resources: container.Resources{
			CPUPeriod:    cgroups.CPUPeriod,
			CPUQuota:     int64(l.CPUQuota * cgroups.CPUPeriod),
			CgroupParent: l.ParentCgroupID,
			Memory:       l.CgroupMemorySize.Int64(),
		},
		ReadonlyRootfs: true,
	}
	// this should be ignored since we're using "host"
	networkingConfig := &network.NetworkingConfig{}

	// this is the name we'll use to start and stop the container
	containerName := l.ServiceID()

	_, err := l.DockerClient.ContainerCreate(ctx, containerConfig, hostConfig, networkingConfig, containerName)
	if err != nil {
		return util.Errorf("could not create container %s: %s", containerName, err)
	}

	err = l.DockerClient.ContainerStart(ctx, containerName, types.ContainerStartOptions{})
	if err != nil {
		return util.Errorf("could not start container %s: %s", containerName, err)
	}
	return nil
}

func (l *Launchable) MakeCurrent() error {
	// there is no current symlink to flip for docker containers
	return nil
}

func (l *Launchable) PostActivate() (string, error) {
	// post activate is not supported for docker containers
	return "", nil
}

func (l *Launchable) PostInstall() (string, error) {
	// there is no post install step for docker containers
	return "", nil
}

func (l *Launchable) Prune(size.ByteCount) error {
	// no pruning for docker for now at least
	return nil
}

func (l *Launchable) RestartPolicy() runit.RestartPolicy {
	return l.RestartPolicy_
}

func (l *Launchable) ServiceID() string {
	return l.ServiceID_
}

func (l *Launchable) Stop(_ *runit.ServiceBuilder, _ runit.SV, _ bool) error {
	// TODO: not implemented
	if l.DockerClient == nil {
		return util.Errorf("cannot stop container: docker client is not initialized")
	}

	err := l.DockerClient.ContainerStop(context.TODO(), l.ServiceID(), &l.RestartTimeout)
	if err != nil {
		return util.Errorf("could not stop docker container %s: %s", l.ServiceID(), err)
	}

	// TODO: any value in keeping container around?
	// TODO: any of these options desired?
	err = l.DockerClient.ContainerRemove(context.TODO(), l.ServiceID(), types.ContainerRemoveOptions{})
	if err != nil {
		return util.Errorf("could not rm docker container %s: %s", l.ServiceID(), err)
	}
	return nil
}

func (*Launchable) Type() string {
	return "docker"
}
