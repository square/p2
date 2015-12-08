package cgroups

import (
	"testing"

	"github.com/square/p2/pkg/util/size"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
	"github.com/square/p2/Godeps/_workspace/src/gopkg.in/yaml.v2"
)

func TestMarshalIntegerByteCount(t *testing.T) {
	integeredMemory := []byte(`memory: 500`)
	config := Config{}
	err := yaml.Unmarshal(integeredMemory, &config)
	Assert(t).IsNil(err, "Should not have erred unmarshaling")
	Assert(t).AreEqual(config.Memory, size.ByteCount(500), "Should have unmarshaled the integer representation of bytes")
}

func TestMarshalStringByteCount(t *testing.T) {
	integeredMemory := []byte(`memory: 500G`)
	config := Config{}
	err := yaml.Unmarshal(integeredMemory, &config)
	Assert(t).IsNil(err, "Should not have erred unmarshaling")
	Assert(t).AreEqual(config.Memory, 500*size.Gibibyte, "Should have unmarshaled the integer representation of bytes")
}
