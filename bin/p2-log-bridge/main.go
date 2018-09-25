package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/square/p2/pkg/logbridge"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/version"
	"golang.org/x/sys/unix"
	"gopkg.in/alecthomas/kingpin.v2"
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

	if err := discardSTDOUTAndSTDERR(); err != nil {
		logging.DefaultLogger.WithError(err).Error("fatal error discarding STDOUT and STDERR")
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

// Environments that use systemd-journald may be affected by a bug in which
// logbridge write()s will fail with EPIPE if systemd-journald restarts. To
// avoid this, we can update the STDOUT and STDERR file descriptors to point at
// /dev/null.
func discardSTDOUTAndSTDERR() error {
	dn, err := os.Open(os.DevNull)
	if err != nil {
		return fmt.Errorf("could not open %s: %s", os.DevNull, err)
	}
	dnFd := int(dn.Fd())
	outFd := int(os.Stdout.Fd())
	if err := unix.Dup2(dnFd, outFd); err != nil {
		return fmt.Errorf("could not copy %s to file descriptor %d: %s", os.DevNull, outFd, err)
	}
	errFd := int(os.Stderr.Fd())
	if err := unix.Dup2(dnFd, errFd); err != nil {
		return fmt.Errorf("could not copy %s to file descriptor %d: %s", os.DevNull, errFd, err)
	}
	return nil
}
