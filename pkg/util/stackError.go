package util

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
)

const fileLinePrefixFormat string = "%s:%d: "

// StackError represents an error with an associated stack trace.
type StackError interface {
	error
	Stack() []byte
}

type stackError struct {
	message string
	stack   []byte
}

func (e *stackError) Error() string {
	return e.message
}

func (e *stackError) Stack() []byte {
	return e.stack
}

// Errorf formats according to fmt.Errorf, but prefixes the error
// message with filename and line number.
func Errorf(format string, a ...interface{}) error {
	// Skip one stack frame to get the file & line number of caller.
	if _, file, line, ok := runtime.Caller(1); ok {
		format = fmt.Sprintf(fileLinePrefixFormat, filepath.Base(file), line) + format
	}
	return &stackError{fmt.Sprintf(format, a...), stack(1)}
}

// trimLine returns a subslice of b by slicing off the bytes up to and
// including the first newline. Returns nil if b does not contain a
// newline. This method is safe to call with b==nil.
func trimLine(b []byte) []byte {
	index := bytes.IndexByte(b, '\n')
	if index == -1 {
		return nil
	}
	return b[index+1:]
}

// stack formats the stack trace of the calling goroutine. The
// argument skip is the number of stack frames to skip before recoding
// the stack trace, with 0 identifying the caller of stack.
func stack(skip int) []byte {
	// Grow buf until it's large enough to store the entire stack trace.
	buf := make([]byte, 1024)
	var n int
	for {
		n = runtime.Stack(buf, false)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, len(buf)*2)
	}

	// Skip over skip+1 stack frames. The output of runtime.Stack looks
	// like:
	//
	//   goroutine <num> [<state>]:
	//   <func1>(<addr1>)
	//     <file1>:<line1> +<offset1>
	//   <func2>(<addr2>)
	//     <file2>:<line2> +<offset2>
	//   ...
	//
	// We want to keep the first line identifying the goroutine, then
	// skip over skip+1 pairs of lines.
	start := trimLine(buf)
	end := start

	// There is a pair of lines per stack frame.
	for i := 0; i <= skip; i++ {
		end = trimLine(trimLine(end))
	}

	// Copy the bytes starting at "end" to the bytes starting at
	// "start", overwriting the first "skip+1" stack frames.
	copy(start, end)

	// We deleted the bytes between end and start and need to trim the
	// size of the buffer.
	n -= (len(start) - len(end))
	return buf[:n]
}

// Stack formats the stack trace of the calling goroutine.
func Stack() []byte {
	return stack(1)
}
