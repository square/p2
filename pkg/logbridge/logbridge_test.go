package logbridge

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/rcrowley/go-metrics"
	"github.com/square/p2/Godeps/_workspace/src/golang.org/x/time/rate"
	"github.com/square/p2/pkg/logging"
)

const newLine = byte(10)

type TrackingWriter struct {
	numWrites int
}

func (tw *TrackingWriter) Write(p []byte) (int, error) {
	tw.numWrites++
	return len(p), nil
}

func TestLogBridge(t *testing.T) {
	type testCase struct {
		inputSize      int
		bridgeCapacity int
		expected       int
	}

	testCases := []testCase{
		{inputSize: 0, bridgeCapacity: 0, expected: 0},
		{inputSize: 10, bridgeCapacity: 10, expected: 10},
		{inputSize: 100, bridgeCapacity: 10, expected: 10},
		{inputSize: 10, bridgeCapacity: 100, expected: 10},
	}

	for i, testCase := range testCases {
		t.Logf("test case %d", i)
		input := make([]byte, 0, 2*testCase.inputSize)
		for i := 0; i < testCase.inputSize; i++ {
			input = append(input, byte('a'), newLine)
		}

		reader := bytes.NewReader(input)
		writer := &TrackingWriter{}

		metReg := metrics.NewRegistry()
		lb := NewLogBridge(reader,
			ioutil.Discard,
			writer,
			logging.DefaultLogger,
			testCase.inputSize,
			1024,
			metReg,
			"log_lines",
			"log_bytes",
			"dropped_lines",
			"time_spent_throttled_ms")

		lb.LossyCopy(reader, testCase.bridgeCapacity)
		if writer.numWrites < testCase.expected {
			t.Errorf("Writer did not receive enough writes, got %d expected: %d", writer.numWrites, testCase.inputSize)
		}
		if lb.logLinesCount.Count() != int64(writer.numWrites) {
			t.Errorf("log line metric did not get right amount, got %d expected: %d", lb.logLinesCount.Count(), writer.numWrites)
		}
	}
}

// This writer becomes latent after receiving capacity writes.
type LatentWriter struct {
	capacity int
	writes   int
}

func (sw *LatentWriter) Write(p []byte) (n int, err error) {
	if sw.writes > sw.capacity {
		time.Sleep(10 * time.Millisecond)
	}
	sw.writes++

	return len(p), nil
}

func TestLogBridgeLogDrop(t *testing.T) {
	bridgeCapacity := 3
	input := []byte{
		byte('a'), newLine,
		byte('b'), newLine,
		byte('c'), newLine,
		byte('d'), newLine,
		byte('e'), newLine,
		byte('f'), newLine}
	reader := bytes.NewReader(input)

	writer := &LatentWriter{}
	metReg := metrics.NewRegistry()
	lb := NewLogBridge(reader,
		ioutil.Discard,
		writer,
		logging.DefaultLogger,
		bridgeCapacity*10,
		1024,
		metReg,
		"log_lines",
		"log_bytes",
		"dropped_lines",
		"time_spent_throttled_ms")
	lb.LossyCopy(reader, bridgeCapacity)

	if writer.writes < bridgeCapacity {
		t.Errorf("Expected at least %d messages to succeed under writer latency, got %d.", bridgeCapacity, writer.writes)
	}
}

// This writer returns errors of type (*ErrorWriter).err after numSuccess calls to Write
type errorWriter struct {
	numSuccess int
	writes     int
	bytes      []byte
}

type transientErrorWriter struct {
	numSuccess int
	writes     int
	bytes      []byte
}

func (ew *errorWriter) Write(p []byte) (n int, err error) {
	if ew.writes > ew.numSuccess {
		return 0, errors.New("boom")
	}

	ew.writes++

	ew.bytes = append(ew.bytes, p...)

	return len(p), nil
}

func (tew *transientErrorWriter) Write(p []byte) (n int, err error) {
	tew.writes++

	if tew.writes > tew.numSuccess && tew.writes%2 == 0 {
		return 0, NewRetriableError(errors.New("boom"))
	}

	tew.bytes = append(tew.bytes, p...)
	return len(p), nil
}

func TestErrorCases(t *testing.T) {
	input := []byte{
		byte('a'), newLine,
		byte('b'), newLine,
		byte('c'), newLine,
		byte('d'), newLine,
		byte('e'), newLine,
		byte('f'), newLine}
	bridgeCapacity := 6

	// we're testing errors and want the tests to be fast
	backoff = func(_ int) time.Duration { return time.Duration(0 * time.Millisecond) }

	retriableErrorWriter := &transientErrorWriter{
		numSuccess: 2,
	}

	errorWriter := &errorWriter{
		numSuccess: 4,
	}

	reader := bytes.NewReader(input)
	metReg := metrics.NewRegistry()
	lb := NewLogBridge(reader,
		ioutil.Discard,
		errorWriter,
		logging.DefaultLogger,
		bridgeCapacity,
		bridgeCapacity*10,
		metReg,
		"log_lines",
		"log_bytes",
		"dropped_lines",
		"time_spent_throttled_ms")
	lb.LossyCopy(reader, bridgeCapacity)

	if len(errorWriter.bytes) >= len(input) {
		t.Errorf("Expected non-retriable error to cause line to be skipped.")
	}

	reader = bytes.NewReader(input)
	lb.LossyWriter = retriableErrorWriter
	lb.LossyCopy(reader, bridgeCapacity)

	if len(retriableErrorWriter.bytes) != len(input) {
		t.Errorf(
			"Expected bridge to successfully retry writes that result in error. Expected %d Got %d",
			len(input),
			len(retriableErrorWriter.bytes),
		)
	}
}

func TestIsRetriable(t *testing.T) {
	type testCase struct {
		err         error
		expectation bool
	}

	testCases := []testCase{
		{errors.New("an error"), false},
		{errors.New("an error"), false},
		{&retriableError{errors.New("an error")}, false},
		{retriableError{errors.New("an error")}, true},
		{NewRetriableError(errors.New("an error")), true},
	}

	for i, testCase := range testCases {
		actual := isRetriable(testCase.err)
		if actual != testCase.expectation {
			t.Errorf("test case %d: expected %b got %b", i, testCase.expectation, actual)
		}
	}
}

func TestRateLimiting(t *testing.T) {
	inputSize := 100
	input := make([]byte, 0, inputSize*2)
	for i := 0; i < inputSize; i++ {
		inputLine := []byte{byte((i % 26) + 65), newLine}
		input = append(input, inputLine...)
	}
	fmt.Printf("input: %d", len(input))

	bridgeCapacity := 6
	reader := bytes.NewReader(input)

	lineLimit := 3
	metReg := metrics.NewRegistry()
	lb := NewLogBridge(reader,
		ioutil.Discard,
		ioutil.Discard,
		logging.DefaultLogger,
		lineLimit,
		1024,
		metReg,
		"log_lines",
		"log_bytes",
		"dropped_lines",
		"time_spent_throttled_ms")
	// We're testing these, so we finely control their parameters
	lb.logLineRateLimit = rate.NewLimiter(rate.Limit(inputSize), inputSize)
	lb.logByteRateLimit = rate.NewLimiter(rate.Limit(1024), 1024)
	lb.LossyCopy(reader, bridgeCapacity)

	loggedLines := lb.logLinesCount.Count()
	droppedLines := lb.droppedLineCount.Count()
	if loggedLines == 0 {
		t.Errorf("Expected some logs to get through.")
	}
	if loggedLines == int64(inputSize) {
		t.Errorf("Expected some lines to get dropped")
	}
	if droppedLines == 0 {
		t.Errorf("Expected dropped lines to be non-zero")
	}
}

func fakeMetricRegistry() (MetricsRegistry, metrics.Counter, metrics.Counter) {
	registry := metrics.NewRegistry()
	logLineCounter := metrics.NewCounter()
	logByteCounter := metrics.NewCounter()

	registry.Register("log_lines", logLineCounter)
	registry.Register("log_bytes", logByteCounter)

	return registry, logLineCounter, logByteCounter
}
