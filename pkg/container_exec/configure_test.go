package container_exec

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/square/p2/pkg/opencontainer"
)

const (
	testPodID = "hello-pod"

	// nothing will actually get written to these
	testSecretsPath    = "/foo/bar/secrets"
	testLogDir         = "/foo/bar/log"
	testLaunchableRoot = "/some/launchable/root"
	testPodHome        = "/some/pod/home"

	testUid uint32 = 1234
	testGid uint32 = 1235

	testTemplateFilename = "config.json.template"
)

// just use the TMPDIR already set
var testTmpDir = os.Getenv("TMPDIR")

func TestRewriteRuncConfigHappy(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "test_rewrite_runc_config_template")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	launchableRoot := filepath.Join(tempDir, "launchable_dir")
	err = os.MkdirAll(launchableRoot, 0755)
	if err != nil {
		t.Fatal(err)
	}

	// write the config.json.template file
	err = ioutil.WriteFile(filepath.Join(launchableRoot, testTemplateFilename), []byte(exampleRuncJSON), 0744)
	if err != nil {
		t.Fatal(err)
	}

	configWriter, err := NewRuncConfigWriter(launchableRoot, testTemplateFilename, int(testUid), int(testGid))
	if err != nil {
		t.Fatal(err)
	}

	err = configWriter.AddBindMounts([]string{testSecretsPath, testLogDir, testTmpDir})
	if err != nil {
		t.Fatal(err)
	}

	err = configWriter.Write()
	if err != nil {
		t.Fatal(err)
	}

	// now get the wrtten config.json file
	configJSONPath := filepath.Join(launchableRoot, opencontainer.SpecFilename)
	runcConfigBytes, err := ioutil.ReadFile(configJSONPath)
	if err != nil {
		t.Fatal(err)
	}

	var runcConfig opencontainer.Spec
	err = json.Unmarshal(runcConfigBytes, &runcConfig)
	if err != nil {
		t.Fatal(err)
	}

	if runcConfig.Process.User.UID != testUid {
		t.Errorf("expected loaded uid to be %d but was %d", testUid, runcConfig.Process.User.UID)
	}
	if runcConfig.Process.User.GID != testGid {
		t.Errorf("expected loaded gid to be %d but was %d", testGid, runcConfig.Process.User.GID)
	}

	mounts := runcConfig.Mounts
	if len(mounts) != 9 {
		t.Fatalf("expected 9 mounts (6 original + secrets mount + logs mount + tmp mount) but found %d", len(mounts))
	}

	secretsMountFound := false
	logMountFound := false
	tmpMountFound := false
	for _, mount := range mounts {
		if mount.Destination == testSecretsPath {
			secretsMountFound = true
			if mount.Source != testSecretsPath {
				t.Errorf("expected secrets mount to have source of %s but was %s", testSecretsPath, mount.Source)
			}
			if mount.Type != "bind" {
				t.Errorf("expected secrets mount to have the \"bind\" type but was %s", mount.Type)
			}
		}

		if mount.Destination == testLogDir {
			logMountFound = true
			if mount.Source != testLogDir {
				t.Errorf("expected log mount to have source of %s but was %s", testLogDir, mount.Source)
			}
			if mount.Type != "bind" {
				t.Errorf("expected log mount to have the \"bind\" type but was %s", mount.Type)
			}
		}

		if mount.Destination == testTmpDir {
			tmpMountFound = true
			if mount.Source != testTmpDir {
				t.Errorf("expected tmp mount to have source of %s but was %s", testTmpDir, mount.Source)
			}
			if mount.Type != "bind" {
				t.Errorf("expected tmp mount to have the \"bind\" type but was %s", mount.Type)
			}
		}
	}

	if !secretsMountFound {
		t.Errorf("the secrets mount was not added")
	}
	if !logMountFound {
		t.Errorf("the log mount was not added")
	}
	if !tmpMountFound {
		t.Errorf("the tmp mount was not added")
	}
}

var exampleRuncJSON = `
{
        "ociVersion": "1.0.0-rc5",
        "platform": {
                "os": "linux",
                "arch": "amd64"
        },
        "process": {
                "terminal": false,
                "consoleSize": {
                        "height": 0,
                        "width": 0
                },
                "user": {
                        "uid": 0,
                        "gid": 0
                },
                "args": [
                        "/usr/local/bin/foo",
                        "-c",
                        "/etc/foo-config.yaml"
                ],
                "env": [
                        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                        "TERM=xterm"
                ],
                "cwd": "/",
                "capabilities": {
                        "bounding": [],
                        "effective": [],
                        "inheritable": [],
                        "permitted": [],
                        "ambient": []
                },
                "rlimits": [
                ],
                "noNewPrivileges": true
        },
        "root": {
                "path": "rootfs",
                "readonly": true
        },
        "hostname": "runc",
        "mounts": [
                {
                        "destination": "/proc",
                        "type": "proc",
                        "source": "proc"
                },
                {
                        "destination": "/dev",
                        "type": "tmpfs",
                        "source": "tmpfs",
                        "options": [
                                "nosuid",
                                "strictatime",
                                "mode=755",
                                "size=65536k"
                        ]
                },
                {
                        "destination": "/dev/pts",
                        "type": "devpts",
                        "source": "devpts",
                        "options": [
                                "nosuid",
                                "noexec",
                                "newinstance",
                                "ptmxmode=0666",
                                "mode=0620",
                                "gid=5"
                        ]
                },
                {
                        "destination": "/dev/shm",
                        "type": "tmpfs",
                        "source": "shm",
                        "options": [
                                "nosuid",
                                "noexec",
                                "nodev",
                                "mode=1777",
                                "size=65536k"
                        ]
                },
                {
                        "destination": "/dev/mqueue",
                        "type": "mqueue",
                        "source": "mqueue",
                        "options": [
                                "nosuid",
                                "noexec",
                                "nodev"
                        ]
                },
                {
                        "destination": "/sys",
                        "type": "sysfs",
                        "source": "sysfs",
                        "options": [
                                "nosuid",
                                "noexec",
                                "nodev",
                                "ro"
                        ]
                }
        ],
        "linux": {
                "resources": {
                        "devices": [
                                {
                                        "allow": false,
                                        "access": "rwm"
                                }
                        ]
                },
                "namespaces": [
                        {
                                "type": "pid"
                        },
                        {
                                "type": "ipc"
                        },
                        {
                                "type": "uts"
                        },
                        {
                                "type": "mount"
                        }
                ],
                "maskedPaths": [
                        "/proc/kcore",
                        "/proc/latency_stats",
                        "/proc/timer_list",
                        "/proc/timer_stats",
                        "/proc/sched_debug",
                        "/sys/firmware"
                ],
                "readonlyPaths": [
                        "/proc/asound",
                        "/proc/bus",
                        "/proc/fs",
                        "/proc/irq",
                        "/proc/sys",
                        "/proc/sysrq-trigger"
                ]
        }
}`
