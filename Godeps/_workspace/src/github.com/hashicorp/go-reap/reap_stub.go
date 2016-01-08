// +build windows solaris

package reap

// IsSupported returns true if child process reaping is supported on this
// platform. This Windows version always returns false.
func IsSupported() bool {
	return false
}

// ReapChildren is not supported on Windows so this always returns right away.
func ReapChildren(pids PidCh, errors ErrorCh, done chan struct{}) {
}
