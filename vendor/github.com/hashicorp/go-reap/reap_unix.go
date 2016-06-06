// +build !windows,!solaris

package reap

import (
	"os"
	"os/signal"

	"golang.org/x/sys/unix"
)

// IsSupported returns true if child process reaping is supported on this
// platform.
func IsSupported() bool {
	return true
}

// ReapChildren is a long-running routine that blocks waiting for child
// processes to exit and reaps them, reporting reaped process IDs to the
// optional pids channel and any errors to the optional errors channel.
func ReapChildren(pids PidCh, errors ErrorCh, done chan struct{}) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, unix.SIGCHLD)

	for {
	WAIT:
		// Block for an incoming signal that a child has exited.
		select {
		case <-c:
			// Got a child signal, drop out and reap.
		case <-done:
			return
		}

		// Try to reap children until there aren't any more. We never
		// block in here so that we are always responsive to signals, at
		// the expense of possibly leaving a child behind if we get
		// here too quickly. Any stragglers should get reaped the next
		// time we see a signal, so we won't leak in the long run.
		for {
		POLL:
			var status unix.WaitStatus
			pid, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
			switch err {
			case nil:
				// Got a child, clean this up and poll again.
				if pids != nil {
					pids <- pid
				}
				goto POLL

			case unix.ECHILD:
				// No more children, we are done.
				goto WAIT

			case unix.EINTR:
				// We got interrupted, try again. This likely
				// can't happen since we are calling Wait4 in a
				// non-blocking fashion, but it's good to be
				// complete and handle this case rather than
				// fail.
				goto POLL

			default:
				// We got some other error we didn't expect.
				// Wait for another SIGCHLD so we don't
				// potentially spam in here and chew up CPU.
				if errors != nil {
					errors <- err
				}
				goto WAIT
			}
		}
	}
}
