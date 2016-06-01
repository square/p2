package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/sys/unix"
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

	if err := setSTDINToBlock(); err != nil {
		logging.DefaultLogger.WithError(err).Error("fatal error setting STDIN to block")
		os.Exit(1)
	}

	var wg sync.WaitGroup
	loggerCmd := exec.Command(*durableLogger)
	durablePipe, err := loggerCmd.StdinPipe()
	if err != nil {
		logging.DefaultLogger.WithError(err).Error("fatal error during configuration of subordinate log command")
	}

	wg.Add(1)
	go func(loggerCmd exec.Cmd) {
		defer wg.Done()

		if err := loggerCmd.Start(); err != nil {
			logging.DefaultLogger.WithError(err).Errorln("fatal error during execution of subordinate log command")
			os.Exit(1)
		}

		err := loggerCmd.Wait()
		if err != nil {
			logging.DefaultLogger.WithError(err).Errorln("fatal error in subordinate log command")
			os.Exit(1)
		}
	}(*loggerCmd)

	wg.Add(1)
	go func(r io.Reader, durableWriter, lossyWriter io.Writer, logger logging.Logger) {
		defer wg.Done()

		lb := logbridge.NewLogBridge(r, durableWriter, lossyWriter, logger, 1024, 4096, nil, "log_lines", "log_bytes", "dropped_lines", "throttled_ms")

		lb.Tee()
		logging.DefaultLogger.NoFields().Infoln("logbridge Tee returned. Shutting down subordinate log command.")
		durablePipe.Close()
	}(os.Stdin, durablePipe, os.Stdout, logging.DefaultLogger)

	logging.DefaultLogger.NoFields().Info("logging running in background…")
	wg.Wait()
}

// In environments where svlogd is used, the pipe that becomes STDIN of this
// program can be non-blocking. Go's File implementation does not play well
// with non-blocking pipes, in particular it does not recover from an EAGAIN
// error from read(2).
// This function defensively sets its 0th file descriptor to be blocking so
// that we do not have to handle EAGAIN errors.
func setSTDINToBlock() error {
	oldflags, _, errno := unix.Syscall(unix.SYS_FCNTL, 0, unix.F_GETFL, 0)
	if errno != 0 {
		return fmt.Errorf("unix.FCNTL F_GETFL errno: %d", errno)
	}
	_, _, errno = unix.Syscall(unix.SYS_FCNTL, 0, unix.F_SETFL, oldflags&^unix.O_NONBLOCK)
	if errno != 0 {
		return fmt.Errorf("unix.FCNTL F_SETFL errno: %d", errno)
	}
	return nil
}
