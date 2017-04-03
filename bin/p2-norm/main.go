// p2-norm is a CLI tool for printing a normalized pod manifest.
package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/manifest"
)

const helpMessage = `
Read a pod manifest and print it in a normalized format without any signature.
With no filename, or when filename is -, read standard input.
`

var (
	progName = filepath.Base(os.Args[0])
	app      = kingpin.New(progName, helpMessage)
	filename = app.Arg("filename", `Pod manifest file to normalize. Use "-" for stdin.`).String()
	write    = app.Flag("write", "write result to source file instead of stdout").Short('w').Bool()
)

func main() {
	kingpin.MustParse(app.Parse(os.Args[1:]))
	logger := log.New(os.Stderr, progName+": ", 0)

	var data []byte
	var err error
	output := os.Stdout
	if *filename == "" || *filename == "-" {
		if *write {
			logger.Fatalln("--write is incompatible with reading from stdin")
		}
		data, err = ioutil.ReadAll(os.Stdin)
	} else {
		data, err = ioutil.ReadFile(*filename)
		if err == nil && *write {
			output, err = os.OpenFile(*filename, os.O_RDWR, 0)
		}
	}
	if err != nil {
		logger.Fatalln(err)
	}

	m, err := manifest.FromBytes(data)
	if err != nil {
		logger.Fatalln(err)
	}
	err = m.GetBuilder().GetManifest().Write(output)
	if err != nil {
		logger.Fatalln(err)
	}
}
