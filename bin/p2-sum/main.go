// p2-sum is a CLI tool for printing the canonical hash of a P2 pod manifest.
//
// The SHA visible in other tools and logs isn't a straight hash of the manifest's
// contents. Instead, the manifest is parsed and re-serialized into a standard form before
// being hashed with SHA256. This command does the same thing to its inputs, letting the
// user see the same thing that P2 does. Results are printed in a similar format to the
// standard utilities "md5sum" and "shasum".
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/square/p2/pkg/store"
)

var help = flag.Bool("help", false, "show program usage")

const usageMsg = `usage: %s [FLAG]... [FILE]...
Print the canonical P2 pod manifest hash for the given files.
With no FILE, or when FILE is -, read standard input.

Flags:
`

func init() {
	flag.Usage = usage
}

func usage() {
	fmt.Fprintf(os.Stderr, usageMsg, filepath.Base(os.Args[0]))
	flag.PrintDefaults()
}

// HashErr is a sum type holding either a hash (string) or an error raised while producing
// the hash. An explicit struct is used instead of a multi-value return so that the hash
// results can be put in a channel e.g. when operating in parallel.
type HashErr struct {
	Hash string
	Err  error
}

// SumBytes parses the given contents of a manifest file and returns its canonical hash.
func SumBytes(data []byte) HashErr {
	m, err := store.FromBytes(data)
	if err != nil {
		return HashErr{"", err}
	}
	sha, err := m.SHA()
	if err != nil {
		return HashErr{"", err}
	}
	return HashErr{sha, nil}
}

// SumFile returns the canonical hash for the given pod manifest file.
func SumFile(filename string) HashErr {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return HashErr{"", err}
	}
	return SumBytes(data)
}

// SumStdin reads a manifest file from stdin and returns its canonical hash.
func SumStdin() HashErr {
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return HashErr{"", err}
	}
	return SumBytes(data)
}

func main() {
	flag.Parse()
	progName := filepath.Base(os.Args[0])
	if *help {
		usage()
		os.Exit(0)
	}
	args := flag.Args()
	if len(args) == 0 {
		args = []string{"-"}
	}

	stdinDone := false
	var stdinHash HashErr

	// For now, just hash everything sequentially.
	// TODO: parallel file access
	for _, filename := range args {
		var hash HashErr
		if filename == "-" {
			if !stdinDone {
				stdinHash = SumStdin()
				stdinDone = true
			}
			hash = stdinHash
		} else {
			hash = SumFile(filename)
		}
		if hash.Err != nil {
			fmt.Fprintf(os.Stderr, "%s: %s: %s\n", progName, filename, hash.Err)
		} else {
			fmt.Printf("%s  %s\n", hash.Hash, filename)
		}
	}
}
