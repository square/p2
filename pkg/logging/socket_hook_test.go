package logging

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestSocketHookAlwaysJSON(t *testing.T) {
	logger := NewLogger(logrus.Fields{
		"some_field": "some_value",
	})

	socketDir, err := ioutil.TempDir("", "socket_hook_test")
	if err != nil {
		t.Fatalf("could not create temp dir for logging socket hook test: %s", err)
	}
	defer os.RemoveAll(socketDir)

	socketPath := filepath.Join(socketDir, "logging.sock")
	err = logger.AddHook(OutSocket, socketPath)
	if err != nil {
		t.Fatalf("could not add socket logging hook to logger: %s", err)
	}

	messages := make(chan []byte)
	errors := make(chan error)
	defer close(messages)
	defer close(errors)

	go func() {
		ln, err := net.Listen("unix", socketPath)
		if err != nil {
			errors <- err
			return
		}

		go func() {
			logger.WithFields(logrus.Fields{
				"another_field": "another_value",
			}).Infoln("some message")
		}()

		conn, err := ln.Accept()
		if err != nil {
			errors <- err
			return
		}

		message, err := ioutil.ReadAll(conn)
		if err != nil {
			errors <- err
			return
		}
		messages <- message
	}()

	var rawMessage []byte
	select {
	case err := <-errors:
		t.Fatalf("unexpected error reading from logging socket: %s", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for log message on socket")
	case rawMessage = <-messages:
	}

	var message struct {
		SomeField    string `json:"some_field"`
		AnotherField string `json:"another_field"`

		// this one is added automatically by logrus
		Message string `json:"msg"`
	}
	err = json.Unmarshal(rawMessage, &message)
	if err != nil {
		t.Fatalf("could not decode log message %q from socket as JSON: %s", string(rawMessage), err)
	}

	if message.SomeField != "some_value" {
		t.Errorf("The default logger field of %q didn't have value of %q. Full message: %s", "some_field", "some_value", string(rawMessage))
	}

	if message.AnotherField != "another_value" {
		t.Errorf("The default logger field of %q didn't have value of %q. Full message: %s", "another_field", "another_value", string(rawMessage))
	}

	if message.Message != "some message" {
		t.Errorf("The default logger field of %q didn't have value of %q. Full message: %s", "msg", "some message", string(rawMessage))
	}
}
