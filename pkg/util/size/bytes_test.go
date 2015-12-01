package size

import (
	"fmt"
	"testing"

	. "github.com/square/p2/Godeps/_workspace/src/github.com/anthonybishopric/gotcha"
)

func TestSizeParsing(t *testing.T) {
	tests := map[string]ByteCount{
		"1":        Byte,
		"10":       ByteCount(10),
		"80B":      ByteCount(80),
		"0.001K":   0.001 * Kibibyte,
		"10G":      10 * Gibibyte,
		"100.5 MB": 100.5 * Mebibyte,
		"80T":      80 * Tebibyte,
		"zilch":    ByteCount(-1),
		"1000BB":   ByteCount(-1),
		"1.0.0.0K": ByteCount(-1),
	}

	for str, expected := range tests {
		actual, err := Parse(str)
		if expected == ByteCount(-1) {
			Assert(t).IsNotNil(err, fmt.Sprintf("Should have erred parsing size %s", str))
		} else {
			Assert(t).AreEqual(expected, actual, fmt.Sprintf("Sizes did not match for string %s", str))
		}
	}
}

func TestSizeString(t *testing.T) {
	tests := map[ByteCount]string{
		Byte:             "1B",
		ByteCount(10):    "10B",
		ByteCount(80):    "80B",
		0.001 * Kibibyte: "1B",
		ByteCount(999):   "999B",
		10 * Gibibyte:    "10.0G",
		100.5 * Mebibyte: "100.5M",
		80 * Tebibyte:    "80.0T",
	}

	for value, expected := range tests {
		Assert(t).AreEqual(expected, value.String(), fmt.Sprintf("Strings didn't match %f", value))
	}
}
