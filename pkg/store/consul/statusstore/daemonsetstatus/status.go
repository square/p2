package daemonsetstatus

type Status struct {
	// ManifestSHA is the sha of the manifest that was most recently
	// deployed
	ManifestSHA string `json:"manifest_sha"`

	// NodesDeployed is roughly the number of nodes that have had the manifest
	// corresponding to manifest_sha deployed to them. This number is
	// periodically sampled so that it doesn't need to be written every time a
	// node is scheduled
	NodesDeployed int `json:"nodes_deployed"`

	ReplicationInProgress bool `json:"replication_in_progress"`
}
