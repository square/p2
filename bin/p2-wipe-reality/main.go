package main

import (
	"log"

	"github.com/square/p2/pkg/store/consul"
	"github.com/square/p2/pkg/store/consul/flags"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	nodeName = kingpin.Arg("node", "The node to wipe reality for").Required().String()
	help     = `p2-wipe-reality takes a hostname, a token and recursively deletes all pods
in the reality tree. This is useful if any pods on a host have
been manually altered in some way and need to be restored to
a known state.
`
)

func main() {
	// CLI takes a hostname, a token and recursively deletes all pods
	// in the reality tree. This is useful if any pods on a host have
	// been manually altered in some way and need to be restored to
	// a known state.
	kingpin.Version(version.VERSION)
	_, opts, _ := flags.ParseWithConsulOptions()

	client := consul.NewConsulClient(opts)
	store := consul.NewConsulStore(client)

	pods, _, err := store.ListPods(consul.REALITY_TREE, types.NodeName(*nodeName))
	if err != nil {
		log.Fatalf("Could not list pods for node %v: %v", *nodeName, err)
	}
	for _, pod := range pods {
		log.Printf("Deleting %v from reality\n", pod.Manifest.ID())
		_, err := store.DeletePod(consul.REALITY_TREE, types.NodeName(*nodeName), pod.Manifest.ID())
		if err != nil {
			log.Fatalf("Could not remove %s/%s from pod reality tree: %v", *nodeName, pod.Manifest.ID(), err)
		}
	}
}
