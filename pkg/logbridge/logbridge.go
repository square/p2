package logbridge

import (
	"bufio"
	"bytes"
	"io"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/rcrowley/go-metrics"
	"github.com/square/p2/pkg/logging"
)

type LogBridge struct {
	Reader        io.Reader
	DurableWriter io.Writer
	LossyWriter   io.Writer

	logger        logging.Logger
	metrics       MetricsRegistry
	logLinesCount metrics.Counter
	logBytes      metrics.Counter
}

type MetricsRegistry interface {
	Register(metricName string, metric interface{}) error
}

func NewLogBridge(r io.Reader,
	durableWriter io.Writer,
	lossyWriter io.Writer,
	logger logging.Logger,
	metricsRegistry MetricsRegistry,
	loglineMetricKeyName string,
	logByteMetricKeyName string) *LogBridge {
	if metricsRegistry == nil {
		metricsRegistry = metrics.NewRegistry()
	}
	lineCount := metrics.NewCounter()
	logBytes := metrics.NewCounter()

	_ = metricsRegistry.Register(loglineMetricKeyName, lineCount)
	_ = metricsRegistry.Register(logByteMetricKeyName, logBytes)

	return &LogBridge{
		Reader:        r,
		DurableWriter: durableWriter,
		LossyWriter:   lossyWriter,
		logger:        logger,
		metrics:       metricsRegistry,
		logLinesCount: lineCount,
		logBytes:      logBytes,
	}
}

// LossyCopy implements a buffered copy operation between dest and src.
// It returns the number of dropped messages as a result of insufficient
// capacity
func (lb *LogBridge) LossyCopy(r io.Reader, capacity int) {
	lines := make(chan []byte, capacity)

	go lb.lossyCopy(r, lines)

	var n int
	var err error
	for line := range lines {
		n, err = writeWithRetry(lb.LossyWriter, line, lb.logger)
		if err != nil {
			lb.logger.WithError(err).WithField("dropped line", line).WithField("retried", isRetriable(err)).WithField("bytes written", n).Errorln("Encountered a non-recoverable error. Proceeding.")
			continue
		}
		lb.logLinesCount.Inc(1)
		lb.logBytes.Inc(int64(n))
	}
}

// This function will scan lines from src and send them on the lines channel,
// except when the channel is full in which case it will skip the line
func (lb *LogBridge) lossyCopy(r io.Reader, lines chan []byte) {
	defer close(lines)

	droppedLines := 0
	scanner := bufio.NewScanner(r)
	scanner.Split(scanFullLines)
	var buf []byte
	for scanner.Scan() {
		rawLine := scanner.Bytes() // consume a line regardless of the state of the writer

		// The token slices returned by the Scanner are potentially backed by the same
		// array, whose contents changes over time as new input is read. Since the lines
		// will be handled asynchronously, we have to make a copy now. Copy into a large
		// buffer to prevent too much churn on the garbage collector.
		if len(rawLine) > len(buf) || len(buf) == 0 {
			buf = make([]byte, bufio.MaxScanTokenSize)
		}
		n := copy(buf, rawLine)
		line := buf[:n]
		buf = buf[n:]

		select {
		case lines <- line:
		default:
			droppedLines++

			warningMessage := "Dropped was dropped due to full capacity. If this occurs frequently, consider increasing the capacity of this logbridge."
			lb.logger.WithField("dropped line", line).Errorln(warningMessage)
			if droppedLines%10 == 0 {
				select {
				case lines <- []byte(warningMessage):
				case <-time.After(100 * time.Millisecond):
					// best effort warning of dropped messages. If this doesn't succeed expediently, forget it and get back to work
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		lb.logger.WithError(err).Errorln("Encountered error while reading from src. Proceeding.")
	}
}

// scanFullLines is a SplitFunc for a bufio.Scanner that splits at each newline and,
// unlike the the default splitter, returns the entire line with a trailing newline. This
// method is derived from bufio.ScanLines.
func scanFullLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[0 : i+1], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}

// Tee will copy to durableWriter without dropping messages. Lines written to
// lossyWriter will be copied best effort with respect to latency on the
// writer. Writes to lossyWriter are buffered through a go channel.
func (lb *LogBridge) Tee() {
	tr := io.TeeReader(lb.Reader, lb.DurableWriter)

	lb.LossyCopy(tr, 1<<10)
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
