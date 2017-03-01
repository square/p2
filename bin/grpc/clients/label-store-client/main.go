package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/square/p2/pkg/grpc/label_store/client"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"

	"google.golang.org/grpc"
	"gopkg.in/alecthomas/kingpin.v2"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	cmdWatchMatchesText = "watch-matches"
)

var (
	address = kingpin.Flag("address", "Address of the p2-store-server to talk to. Defaults to localhost:3000").Default("localhost:3000").String()

	cmdWatchMatches = kingpin.Command(cmdWatchMatchesText, "Watch the matches for a label selector")
	labelType       = cmdWatchMatches.Flag("label-type", "The type of label watch to do, e.g. pod.").Required().String()
	selector        = cmdWatchMatches.Flag("selector", "A kubernetes style label selector").Required().String()
	numFetches      = cmdWatchMatches.Flag("num-fetches", "The number of watch iterations to display before exiting").Short('n').Default("1").Int()

	logger = log.New(os.Stderr, "", 0)
)

func main() {
	cmd := kingpin.Parse()

	conn, err := grpc.Dial(*address, grpc.WithBlock(), grpc.WithTimeout(5*time.Second), grpc.WithInsecure())
	if err != nil {
		logger.Fatal(err)
	}
	defer conn.Close()

	grpcLabelClient := client.NewClient(conn, logging.DefaultLogger)

	switch cmd {
	case cmdWatchMatchesText:
		watchMatches(grpcLabelClient)
	}
}

type matchWatcher interface {
	WatchMatches(selector klabels.Selector, labelType labels.Type, quitCh <-chan struct{}) (chan []labels.Labeled, error)
}

func watchMatches(watcher matchWatcher) {
	lType, err := labels.AsType(*labelType)
	if err != nil {
		logger.Fatal(err)
	}

	sel, err := klabels.Parse(*selector)
	if err != nil {
		logger.Fatal(err)
	}

	quitCh := make(chan struct{})
	outCh, err := watcher.WatchMatches(sel, lType, quitCh)
	if err != nil {
		logger.Fatal(err)
	}

	for i := 0; i < *numFetches; i++ {
		matches := <-outCh
		outBytes, err := json.Marshal(matches)
		if err != nil {
			logger.Fatal(err)
		}

		fmt.Println(string(outBytes))
	}

	close(quitCh)
}
