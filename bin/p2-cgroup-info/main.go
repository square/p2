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

// scanCgroup scans the root hierarchy of a cgroup memory controller and identifies
// launchables based on P2's naming scheme.
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
		// All of P2's cgroup names follow the naming pattern: "{pod}__{launchable}". See
		// Pod.getLaunchable().
		parts := strings.Split(fileInfo.Name(), "__")
		if len(parts) != 2 {
			continue
		}
		procs, err := ioutil.ReadFile(filepath.Join(dirname, fileInfo.Name(), "cgroup.procs"))
		if err != nil {
			return nil, err
		}
		if len(procs) == 0 {
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
	sys, err := cgroups.Find()
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
