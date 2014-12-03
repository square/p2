package logging

import (
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
	. "github.com/anthonybishopric/gotcha"
)

func TestLoggingCanMergeFields(t *testing.T) {
	fields1 := logrus.Fields{
		"foo": "a",
		"bar": "b",
	}
	fields2 := logrus.Fields{
		"foo": "z",
		"baz": "q",
	}
	res := Merge(fields1, fields2)
	Assert(t).AreEqual("z", res["foo"], "Should have taken new value's foo")
	Assert(t).AreEqual("b", res["bar"], "Should have taken old value's bar")
	Assert(t).AreEqual("q", res["baz"], "Should have taken old value's baz")
}

func TestSubLoggerMergesFields(t *testing.T) {
	logger := NewLogger(logrus.Fields{
		"foo": "a",
		"bar": "b",
	})
	sub := logger.SubLogger(logrus.Fields{
		"foo": "z",
		"baz": "q",
	})
	res := sub.baseFields
	Assert(t).AreEqual("z", res["foo"], "Should have taken new value's foo")
	Assert(t).AreEqual("b", res["bar"], "Should have taken old value's bar")
	Assert(t).AreEqual("q", res["baz"], "Should have taken old value's baz")

	Assert(t).AreEqual("a", logger.baseFields["foo"], "Should not have overwritten original")
}

func TestLoggingMergeDoesNotModifyOriginalMap(t *testing.T) {
	fields1 := logrus.Fields{
		"foo": "a",
	}
	fields2 := logrus.Fields{
		"foo": "b",
	}
	Merge(fields1, fields2)

	Assert(t).AreEqual("a", fields1["foo"], "Should not have modified the original fields")
}

func TestWithFieldsCombinesBaseFieldsAndGiven(t *testing.T) {
	logger := NewLogger(logrus.Fields{
		"foo": "a",
	})
	entry := logger.WithFields(logrus.Fields{
		"baz": "c",
	})

	Assert(t).AreEqual("a", entry.Data["foo"], "should have kept foo")
	Assert(t).AreEqual("c", entry.Data["baz"], "should have merged baz")
}

func TestAddSocketHook(t *testing.T) {
	logger := NewLogger(logrus.Fields{})
	socket_location := "test_socket.sock"
	os.Remove(socket_location)

	// make goroutine to listen to socket and write what it gets to channel
	out := make(chan []byte)
	go func() {
		l, err := net.Listen("unix", socket_location)
		Assert(t).IsNil(err, "Got an unexpected error when trying to listen to socket")
		defer l.Close()

		fd, err := l.Accept()
		Assert(t).IsNil(err, "Got an unexpected error when trying to call accept() on socket")
		buf := make([]byte, 1024)
		n, err := fd.Read(buf[:])
		Assert(t).IsNil(err, "Got an unexpected error trying to read from socket connection")
		out <- buf[:n]
	}()

	// wait for socket to be set up
	time.Sleep(1 * time.Millisecond)

	// Add socket hook and log something
	err := logger.AddHook(OUT_SOCKET, socket_location)
	Assert(t).IsNil(err, "Got an unexpected error when adding a socket logging hook")
	logger.WithFields(logrus.Fields{}).Error("some message")

	// Just to make extracting json fields easy
	type SocketHookLogMessage struct {
		Message string `json:"msg"`
	}

	select {
	case logMessage := <-out:
		messageStruct := SocketHookLogMessage{}
		err := json.Unmarshal(logMessage, &messageStruct)
		Assert(t).IsNil(err, "Got an unexpected error when unmarshalling the JSON log message")
		Assert(t).AreEqual(messageStruct.Message, "some message", "Did not get the expected log message on the socket")
		break
	case <-time.After(5 * time.Second):
		Assert(t).Fail("Didn't get a message through the socket during the timeout period")
	}
}

func TestAddUnrecognizedHook(t *testing.T) {
	logger := NewLogger(logrus.Fields{})

	err := logger.AddHook("unrecognized_type", "some_destination")
	Assert(t).IsNotNil(err, "Expected an error for adding an unrecognized hook output type")
}
