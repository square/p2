package main

import (
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/square/p2/pkg/config"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/runit"
)

func main() {
	config, err := config.LoadFromEnvironment()
	fatalize(err)
	runitMonCfg, err := config.ReadMap("runit_monitor")
	fatalize(err)
	childDurationStr, err := config.ReadString("child_duration")
	fatalize(err)
	if childDurationStr == "" {
		childDurationStr = "10s"
	}
	childDuration, err := time.ParseDuration(childDurationStr)
	fatalize(err)
	toMonitor, err := runitMonCfg.ReadString("to_monitor")
	fatalize(err)
	runitDir, err := runitMonCfg.ReadString("runit_root")
	fatalize(err)
	if runitDir == "" {
		runitDir = runit.DefaultBuilder.RunitRoot
	}
	statusPort, err := runitMonCfg.ReadString("port")
	fatalize(err)
	if statusPort == "" {
		logging.DefaultLogger.NoFields().Fatalln("Should have assigned a port under the runit_monitor config")
	}

	service := &runit.Service{Name: toMonitor, Path: filepath.Join(runitDir, toMonitor)}
	http.HandleFunc("/_status",
		func(w http.ResponseWriter, r *http.Request) {
			statResult, err := runit.DefaultSV.Stat(service)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
			} else {
				statusString := fmt.Sprintf(`{"status": "%s", "time": "%s"}`, statResult.ChildStatus, statResult.ChildTime)
				if statResult.ChildTime > childDuration && statResult.ChildStatus == runit.STATUS_RUN {
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprintf(w, statusString)
				} else {
					http.Error(w, statusString, http.StatusServiceUnavailable)
				}
			}
		})
	if err = http.ListenAndServe(fmt.Sprintf(":%d", statusPort), nil); err != nil {
		logging.DefaultLogger.WithError(err).Fatalln("Service crashed")
	}
}

func fatalize(err error) {
	if err != nil {
		logging.DefaultLogger.WithError(err).Fatalln("Error during startup")
	}
}
