// package docker implements a docker launchable type
package docker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/docker/docker/api/types/filters"
	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/launch"
	"github.com/square/p2/pkg/runit"
	"github.com/square/p2/pkg/user"
	"github.com/square/p2/pkg/util"
	"github.com/square/p2/pkg/util/size"

	"github.com/docker/docker/api/types"
	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	dockerclient "github.com/docker/docker/client"

	// add this import explicitly for unix
	_ "github.com/opencontainers/runc/libcontainer/system"
)

var _ launch.Launchable = &Launchable{}

const (
	jsonKeyUsername = "_json_key"
)

type Mounts map[string]Mount

type Mount struct {
	Type   string `yaml:"Type"`
	Source string `yaml:"Source"`
	Target string `yaml:"Target"`
}

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

	// The home directory of the pod
	PodHomeDir string

	// Env variables that are written to disk by P2 related to the Pod
	PodEnvDir string

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

	// EntryPoint is the entrypoint to run with the docker container if passed to the manifest
	Entrypoint strslice.StrSlice

	// PostStartCmd is the cmd in the container to run after the container has started
	PostStartCmd []string

	// PreStopCmd is the cmd in the container to run before the container is stopped
	PreStopCmd []string

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
	if len(l.PreStopCmd) == 0 {
		return nil
	}
	ctx := context.Background()
	containerName := l.containerName()
	// TODO: check if we need to set anything else in this
	execConfig := dockertypes.ExecConfig{
		User:         l.RunAs,
		Cmd:          l.PreStopCmd,
		AttachStdout: true,
	}
	resp, err := l.DockerClient.ContainerExecCreate(ctx, containerName, execConfig)
	if err != nil {
		return util.Errorf("could not create PreStop exec configuration for container %s: %s", containerName, err)
	}

	// TODO: check if we need to set anything in this
	execConfig = dockertypes.ExecConfig{}
	hijackedResp, err := l.DockerClient.ContainerExecAttach(ctx, resp.ID, execConfig)
	if err != nil {
		return util.Errorf("could not start PreStop exec process for container %s: %s", containerName, err)
	}
	defer hijackedResp.Close()
	out, err := ioutil.ReadAll(hijackedResp.Reader)
	if err != nil {
		return util.Errorf("error reading output of PreStop exec process for container %s: %s", containerName, err)
	}
	containerExecInspect, err := l.DockerClient.ContainerExecInspect(ctx, resp.ID)
	if err != nil {
		return util.Errorf("error inspecting PreStop exec process for container %s: %s", containerName, err)
	}
	if containerExecInspect.ExitCode != 0 {
		return launch.DisableError{Inner: util.Errorf("%s", out)}
	}
	// TODO: check if there is anything else to do here
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

	ctx := context.Background()

	envVars, err := loadEnvVars(l.PodEnvDir, l.EnvDir())
	if err != nil {
		return util.Errorf("could not load environment variables for container: %s", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return util.Errorf("could not get hostname: %v", hostname)
	}
	dockerUser, err := dockerUser(l.RunAs)
	if err != nil {
		return err
	}
	containerConfig := &container.Config{
		Hostname: hostname,
		User:     dockerUser,
		Env:      envVars,
		Image:    l.Image,
	}
	if len(l.Entrypoint) > 0 {
		containerConfig.Entrypoint = l.Entrypoint
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

	bindings := []string{}
	dockerMountsDir := filepath.Join(l.PodHomeDir, "docker_mounts.d")
	mountFiles, err := ioutil.ReadDir(dockerMountsDir)
	if err != nil && !os.IsNotExist(err) {
		return util.Errorf("could not read from %s dir: %v", dockerMountsDir, err)
	}
	for _, file := range mountFiles {
		mounts := Mounts{}
		if !file.IsDir() {
			// read content and load into bindings var
			mountFilePath := filepath.Join(dockerMountsDir, file.Name())
			data, err := ioutil.ReadFile(mountFilePath)
			if err != nil {
				return util.Errorf("could not read file %s: %v", mountFilePath, err)
			}
			err = yaml.Unmarshal(data, &mounts)
			if err != nil {
				return util.Errorf("error unmarshalling data: %v", err)
			}
			for _, mount := range mounts {
				if mount.Type != "bind" {
					continue
				}
				bindings = append(bindings, fmt.Sprintf("%s:%s", mount.Source, mount.Target))
			}
		}
	}
	hostConfig := &container.HostConfig{
		Binds:       bindings,
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
	containerName := l.containerName()

	_, err = l.DockerClient.ContainerCreate(ctx, containerConfig, hostConfig, networkingConfig, containerName)
	if err != nil {
		return util.Errorf("could not create container %s: %s", containerName, err)
	}

	err = l.DockerClient.ContainerStart(ctx, containerName, types.ContainerStartOptions{})
	if err != nil {
		return util.Errorf("could not start container %s: %s", containerName, err)
	}
	// TODO: check if we need to set anything else in this
	if len(l.PostStartCmd) > 0 {
		execConfig := dockertypes.ExecConfig{
			User:         l.RunAs,
			Cmd:          l.PostStartCmd,
			AttachStdout: true,
		}
		resp, err := l.DockerClient.ContainerExecCreate(ctx, containerName, execConfig)
		if err != nil {
			return util.Errorf("could not create PostStart exec configuration for container %s: %s", containerName, err)
		}

		// TODO: check if we need to set anything in this
		execConfig = dockertypes.ExecConfig{}
		hijackedResp, err := l.DockerClient.ContainerExecAttach(ctx, resp.ID, execConfig)
		if err != nil {
			return util.Errorf("could not start PostStart exec process for container %s: %s", containerName, err)
		}
		defer hijackedResp.Close()
		out, err := ioutil.ReadAll(hijackedResp.Reader)
		if err != nil {
			return util.Errorf("error reading output of PostStart exec process for container %s: %s", containerName, err)
		}
		containerExecInspect, err := l.DockerClient.ContainerExecInspect(ctx, resp.ID)
		if err != nil {
			return util.Errorf("error inspecting PostStart exec process for container %s: %s", containerName, err)
		}
		if containerExecInspect.ExitCode != 0 {
			return launch.EnableError{Inner: util.Errorf("%s", out)}
		}
		// TODO: check if there is anything else to do here
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

// The name we will give to the container related to this launchable
func (l *Launchable) containerName() string {
	return l.ServiceID()
}

func (*Launchable) Type() string {
	return "docker"
}

func GetContainerRegistryAuthStr(jsonKeyFile string) (string, error) {
	if jsonKeyFile == "" {
		return "", util.Errorf("jsonKeyFile arg to GetContainerRegistryAuthStr is not set")
	}

	data, err := ioutil.ReadFile(jsonKeyFile)
	if err != nil {
		return "", util.Errorf("could not read jsonKeyFile %s: %s", jsonKeyFile, err)
	}
	authConfig := dockertypes.AuthConfig{
		Username: jsonKeyUsername,
		Password: string(data),
	}
	encodedJSON, err := json.Marshal(authConfig)
	containerRegistryAuthStr := base64.URLEncoding.EncodeToString(encodedJSON)
	return containerRegistryAuthStr, nil
}

func PruneResources(client *dockerclient.Client) error {
	// TODO: do we care about the prune reports?
	// TODO: do we want to filter what to prune?
	ctx := context.Background()
	pruneFilters := filters.NewArgs()
	_, err := client.ContainersPrune(ctx, pruneFilters)
	if err != nil {
		return util.Errorf("error pruning containers: %s", err)
	}
	_, err = client.ImagesPrune(ctx, pruneFilters)
	if err != nil {
		return util.Errorf("error pruning images: %s", err)
	}
	_, err = client.NetworksPrune(ctx, pruneFilters)
	if err != nil {
		return util.Errorf("error pruning networks: %s", err)
	}
	_, err = client.VolumesPrune(ctx, pruneFilters)
	if err != nil {
		return util.Errorf("error pruning volumes: %s", err)
	}
	return nil
}

func loadEnvVarsFromDir(envDir string) (map[string]string, error) {
	envVars := make(map[string]string)

	envFiles, err := ioutil.ReadDir(envDir)
	if err != nil {
		return nil, util.Errorf("could not read from %s: %v", envDir, err)
	}
	for _, file := range envFiles {
		// we do not expect nested env variables
		if !file.IsDir() {
			// read content and load into return var
			envFilePath := filepath.Join(envDir, file.Name())
			data, err := ioutil.ReadFile(envFilePath)
			if err != nil {
				return nil, util.Errorf("could not read file %s: %v", envFilePath, err)
			}
			envVars[file.Name()] = string(data)
		}
	}

	return envVars, nil
}

// loadEnvVars loads environment variables from the pod env dir and launchable env dir. It expects
// files to be in these directories with the file name being the key of the environment variable
// and the contents of the files being their respective values. In the event of a key conflict
// between the two directories, the launchable one is preferred
func loadEnvVars(podEnvDir string, launchableEnvDir string) ([]string, error) {
	// get pod env vars from disk
	podEnvVars, err := loadEnvVarsFromDir(podEnvDir)
	if err != nil {
		return nil, err
	}
	// get launchable env vars from disk
	launchableEnvVars, err := loadEnvVarsFromDir(launchableEnvDir)
	if err != nil {
		return nil, err
	}

	// deduplicate env vars
	envVarMap := make(map[string]string)
	for k, v := range podEnvVars {
		envVarMap[k] = v
	}
	for k, v := range launchableEnvVars {
		envVarMap[k] = v
	}

	var envVars []string
	for k, v := range envVarMap {
		envVars = append(envVars, fmt.Sprintf("%s=%s", k, v))
	}

	return envVars, nil
}

// converts a username to the <uid>:<gid> format that Docker expects. This obviates the need
// to set up username -> id mappings in a container's /etc/passwd file
func dockerUser(username string) (string, error) {
	uid, gid, err := user.IDs(username)
	if err != nil {
		return "", util.Errorf("could not compute docker formatted uid and gid for username %s: %s", username, err)
	}

	return fmt.Sprintf("%d:%d", uid, gid), nil
}
