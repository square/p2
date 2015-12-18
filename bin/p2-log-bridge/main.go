package main

import (
	"io"
	"os"
	"os/exec"
	"os/signal"

	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/alecthomas/kingpin.v2"
	"github.com/square/p2/pkg/logbridge"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/version"
)

var (
	durableLogger = kingpin.Arg("exec", "An executable that logbridge will log to without dropping messages. If a write to STDIN of this program blocks, logbridge will block.").Required().String()
)

func main() {
	kingpin.Version(version.VERSION)
	kingpin.Parse()

	loggerCmd := exec.Command(*durableLogger)
	durablePipe, err := loggerCmd.StdinPipe()
	if err != nil {
		logging.DefaultLogger.WithError(err).Errorln("failed during logbridge setup")
		os.Exit(1)
	}

	go handleSignals(durablePipe)

	go func(r io.Reader, durableWriter io.Writer, nonDurableWriter io.Writer, logger logging.Logger) {
		logbridge.Tee(r, durableWriter, nonDurableWriter, logger)
	}(os.Stdin, durablePipe, os.Stdout, logging.DefaultLogger)

	loggerCmd.Start()
	err = loggerCmd.Wait()
	if err != nil {
		logging.DefaultLogger.WithError(err)
		os.Exit(1)
	}
}

// This is slightly naive, it could be more narrow in the signals it cares about
func handleSignals(c io.Closer) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals)
	<-signals
	c.Close()
}
