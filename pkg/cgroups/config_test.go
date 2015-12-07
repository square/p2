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
	yaml.Unmarshal(integeredMemory, &config)
	memory, err := config.MemoryByteCount()
	Assert(t).IsNil(err, "Should not have erred getting the byte count")
	Assert(t).AreEqual(memory, size.ByteCount(500), "Should have unmarshaled the integer representation of bytes")
}

func TestMarshalStringByteCount(t *testing.T) {
	integeredMemory := []byte(`memory: 500G`)
	config := Config{}
	yaml.Unmarshal(integeredMemory, &config)
	memory, err := config.MemoryByteCount()
	Assert(t).IsNil(err, "Should not have erred getting the byte count")
	Assert(t).AreEqual(memory, 500*size.Gibibyte, "Should have unmarshaled the integer representation of bytes")
}
