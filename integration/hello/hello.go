package main

import (
	"flag"
	"fmt"
	"html"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	osuser "os/user"
	"strconv"
	"strings"

	"gopkg.in/yaml.v2"
)

var port = flag.String("port", "", "port that hello should listen on")

// verifyFile is used for testing the bind mounting functionality of P2 for
// opencontainers. The integration test can write a file into a directory, bind
// mount it into the container, and configure hello to crash if it can't see
// the file
var verifyFile = flag.String("verifyFile", "", "file that hello should verify exists at startup prior to starting server")

var verifyUser = flag.String("verifyUser", "", "instructs hello to verify that it is running as the passed user before responding to HTTP requests")

func SayHello(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("%q %q", r.Method, r.URL.Path)
	fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
}

// Expects a URL like /exit/5, which means that the process
// should os.Exit(5)
// This endpoint is useful for testing the preparer's ability to capture
// the exit code of a process it starts. The integration test uses this
// endpoint to make the hello pod exit with a random code.
func Exit(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	exitCode, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		http.Error(
			w,
			fmt.Sprintf("Could not convert %s to an integer: %s", parts[len(parts)-1], err),
			http.StatusBadRequest,
		)
		return
	}

	os.Exit(exitCode)
}

type HelloConfig struct {
	Port int `yaml:"port"`
}

func main() {
	flag.Parse()
	if *port == "" {
		filePath := os.Getenv("CONFIG_PATH")
		if filePath == "" {
			log.Fatal("$CONFIG_PATH was not set")
		}

		configBytes, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Fatal(err)
		}

		var config HelloConfig
		err = yaml.Unmarshal(configBytes, &config)
		if err != nil {
			log.Fatal(err)
		}

		if config.Port == 0 {
			log.Fatal("Config must contain a port to run on")
		}

		*port = fmt.Sprintf(":%d", config.Port)
	} else {
		*port = ":" + *port
	}

	if *verifyFile != "" {
		_, err := os.Stat(*verifyFile)
		switch {
		case os.IsNotExist(err):
			fmt.Printf("%s does not exist, was set as -verifyFile. Exiting\n", *verifyFile)
		case err != nil:
			fmt.Printf("could not stat %s (-verifyFile): %s\n", *verifyFile, err)
		}
	}

	if *verifyUser != "" {
		currentUser, err := osuser.Current()
		if err != nil {
			log.Fatalf("could not determine current user: %s", err)
		}

		if currentUser.Username != *verifyUser {
			log.Fatalf("expected to be running as %s but was %s", *verifyUser, currentUser.Username)
		}
	}

	fmt.Printf("Hello is listening at %q", port)
	http.HandleFunc("/", SayHello)
	http.HandleFunc("/exit/", Exit)
	s := &http.Server{
		Addr: *port,
	}
	log.Fatal(s.ListenAndServe())
}
