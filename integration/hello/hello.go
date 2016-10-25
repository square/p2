package main

import (
	"fmt"
	"html"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"gopkg.in/yaml.v2"
)

func SayHello(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("%q %q", r.Method, r.URL.Path)
	fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
}

type HelloConfig struct {
	Port int `yaml:"port"`
}

func main() {
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

	port := fmt.Sprintf(":%d", config.Port)
	fmt.Printf("Hello is listening at %q", port)
	http.HandleFunc("/", SayHello)
	s := &http.Server{
		Addr: port,
	}
	log.Fatal(s.ListenAndServe())
}
