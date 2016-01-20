package logbridge

import (
	"bufio"
	"io"
	"time"

	"github.com/square/p2/pkg/logging"
)

// Copy implements a buffered copy operation between dest and src.
// It returns the number of dropped messages as a result of insufficient
// capacity
func LossyCopy(dest io.Writer, src io.Reader, capacity int, logger logging.Logger) {
	lines := make(chan []byte, capacity)

	go lossyCopy(src, lines, logger)

	var n int
	var err error
	for line := range lines {
		n, err = writeWithRetry(dest, line, logger)
		if err != nil {
			logger.WithError(err).WithField("dropped line", line).WithField("retried", isRetriable(err)).WithField("bytes written", n).Errorln("Encountered a non-recoverable error. Proceeding.")
		}
	}
}

// This function will scan lines from src and send them on the lines channel,
// except when the channel is full in which case it will skip the line
func lossyCopy(src io.Reader, lines chan []byte, logger logging.Logger) {
	defer close(lines)

	droppedLines := 0
	scanner := bufio.NewScanner(src)
	var line []byte
	for scanner.Scan() {
		line = scanner.Bytes() // consume a line regardless of the state of the writer
		select {
		case lines <- line:
		default:
			droppedLines++

			warningMessage := "Dropped was dropped due to full capacity. If this occurs frequently, consider increasing the capacity of this logbridge."
			logger.WithField("dropped line", line).Errorln(warningMessage)
			if droppedLines%10 == 0 {
				select {
				case lines <- []byte(warningMessage):
				case <-time.After(100 * time.Millisecond):
					// best effort warning of dropped messages. If this doesn't suceed expediently, forget it and get back to work
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		logger.WithError(err).Errorln("Encountered error while reading from src. Proceeding.")
	}
}

// Tee will copy to durableWriter without dropping messages, it will copy
// through a buffer to better handle mismatched latencies. Lines written to
// lossyWriter will be copied in a best effort way with respect to latency and
// buffered through a go channel.
func Tee(r io.Reader, durableWriter io.Writer, lossyWriter io.Writer, logger logging.Logger) {
	tr := io.TeeReader(r, bufio.NewWriterSize(durableWriter, 1<<10))

	LossyCopy(lossyWriter, tr, 1<<10, logger)
}

// This is an error wrapper type that may be used to denote an error is retriable
// RetriableError is exported so clients of this package can express their
// error semantics to this package
type retriableError struct {
	err error
}

func NewRetriableError(err error) retriableError {
	return retriableError{err}
}

func (r retriableError) Error() string {
	return r.err.Error()
}

func isRetriable(err error) bool {
	_, ok := err.(retriableError)
	return ok
}

var backoff = func(i int) time.Duration {
	return time.Duration(1 << uint(i) * time.Second)
}

func writeWithRetry(w io.Writer, line []byte, logger logging.Logger) (int, error) {
	var err error
	var n int
	totalAttempts := 5

	for attempt := 1; attempt <= totalAttempts; attempt++ {
		n, err = w.Write(line)
		if err == nil || !isRetriable(err) {
			return n, err
		}
		logger.WithError(err).Errorf("Retriable error, retry %d of %d", attempt, totalAttempts)
		time.Sleep(backoff(attempt))
	}

	return n, err
}
