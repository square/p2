package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/square/p2/pkg/cgroups"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

var app = kingpin.New(
	"p2-cgroup-info",
	"p2-cgroup-info displays which cgroups are running P2 launchables.",
)

// Launchable is the info structure that will be printed for each launchable.
type Launchable struct {
	Node       string `json:"node"`
	Pod        string `json:"pod"`
	Launchable string `json:"launchable"`
	Cgroup     string `json:"cgroup"`
}

// Output is the final output structure that will be printed.
type Output struct {
	Launchables []Launchable `json:"launchables,omitempty"`
}

func hasProcs(cgroupPath string) (bool, error) {
	procs, err := ioutil.ReadFile(filepath.Join(cgroupPath, "cgroup.procs"))
	if err != nil {
		return false, err
	}
	return len(procs) > 0, nil
}

// scanNestedCgroup scans through a nested cgroup naming scheme, e.g.:
// "/cgroup/cpu/p2/mynode/mypod/mylaunchable/cgroup.procs"
func scanNestedCgroup(p2Root string) ([]Launchable, error) {
	var ls []Launchable

	nodes, err := ioutil.ReadDir(p2Root) // "./p2"
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		if !node.IsDir() {
			continue
		}
		nodePath := filepath.Join(p2Root, node.Name()) // "./p2/mynode"
		pods, err := ioutil.ReadDir(nodePath)
		if err != nil {
			return nil, err
		}
		for _, pod := range pods {
			if !pod.IsDir() {
				continue
			}
			podPath := filepath.Join(nodePath, pod.Name()) // "./p2/mynode/mypod"
			launchables, err := ioutil.ReadDir(podPath)
			if err != nil {
				return nil, err
			}
			for _, launchable := range launchables {
				if !launchable.IsDir() {
					continue
				}
				launchablePath := filepath.Join(podPath, launchable.Name()) // "./p2/mynode/mypod/mylaunchable"
				if ok, err := hasProcs(launchablePath); err != nil {
					return nil, err
				} else if !ok {
					continue
				}
				ls = append(ls, Launchable{
					Node:       node.Name(),
					Pod:        pod.Name(),
					Launchable: launchable.Name(),
					Cgroup:     filepath.Join("p2", node.Name(), pod.Name(), launchable.Name()),
				})
			}
		}
	}

	return ls, nil
}

// scanCgroup scans the root hierarchy of a cgroup memory controller and identifies
// launchables based on P2's naming schemes.
//
// Flat names ("/cgroup/cpu/mypod__mylaunchable") are handled here in this method. If
// the scan finds the root of a p2 hierarchy ("/cgroup/cpu/p2/..."), the nested names
// are offloaded to another method.
func scanCgroup(dirname string) (*Output, error) {
	var launchables []Launchable

	// By default, the hostname is used as the node name. The actual node name isn't
	// discoverable from inside the cgroup hierarchy.
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	files, err := ioutil.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	for _, fileInfo := range files {
		if !fileInfo.IsDir() {
			continue
		}
		// The nested naming scheme roots all cgroups in one "p2" directory.
		if fileInfo.Name() == "p2" {
			ls, err := scanNestedCgroup(filepath.Join(dirname, fileInfo.Name()))
			if err != nil {
				return nil, err
			}
			launchables = append(launchables, ls...)
			continue
		}
		// The flat naming scheme follows the naming pattern: "{pod}__{launchable}"
		parts := strings.Split(fileInfo.Name(), "__")
		if len(parts) != 2 {
			continue
		}
		if ok, err := hasProcs(filepath.Join(dirname, fileInfo.Name())); err != nil {
			return nil, err
		} else if !ok {
			continue
		}
		launchables = append(launchables, Launchable{
			Node:       hostname,
			Pod:        parts[0],
			Launchable: parts[1],
			Cgroup:     fileInfo.Name(),
		})
	}

	return &Output{
		Launchables: launchables,
	}, nil
}

func main() {
	app.Version(version.VERSION)
	kingpin.MustParse(app.Parse(os.Args[1:]))
	logger := log.New(os.Stderr, "", 0)

	// P2 only uses the "memory" and "cpu" resource controllers, and it creates an
	// identical hierarchy in either. Just scan "cpu".
	sys, err := cgroups.DefaultSubsystemer.Find()
	if err != nil {
		logger.Fatalf("error finding cgroups: %v", err)
	}
	output, err := scanCgroup(sys.CPU)
	if err != nil {
		logger.Fatalf("error scanning cpu controller: %v", err)
	}
	data, err := json.Marshal(&output)
	if err != nil {
		logger.Fatalf("error formatting output: %v", err)
	}
	_, err = os.Stdout.Write(data)
	if err != nil {
		logger.Fatalf("error writing output: %v", err)
	}
	_, _ = os.Stdout.Write([]byte("\n"))
}
