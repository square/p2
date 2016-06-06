package size

import (
	"fmt"
	"testing"

	"gopkg.in/yaml.v2"

	. "github.com/anthonybishopric/gotcha"
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

type Container struct {
	Count ByteCount
}

// Should accept integer representation
func TestYAMLInt(t *testing.T) {
	t.Parallel()
	var yt Container
	err := yaml.Unmarshal([]byte(`count: 5`), &yt)
	Assert(t).IsNil(err, "unexpected unmarshal error")
	Assert(t).AreEqual(ByteCount(5), yt.Count, "error parsing integer representation")
}

// Should accept quoted string without a suffix
func TestYAMLStr(t *testing.T) {
	t.Parallel()
	var yt Container
	err := yaml.Unmarshal([]byte(`count: "6"`), &yt)
	Assert(t).IsNil(err, "unexpected unmarshal error")
	Assert(t).AreEqual(ByteCount(6), yt.Count, "error parsing string representation")
}

// Should accept quoted string with a suffix
func TestYAMLStrUnit(t *testing.T) {
	t.Parallel()
	var yt Container
	err := yaml.Unmarshal([]byte(`count: "7K"`), &yt)
	Assert(t).IsNil(err, "unexpected unmarshal error")
	Assert(t).AreEqual(ByteCount(7168), yt.Count, "error parsing string suffix")
}

// Should accept an unquoted string with a suffix
func TestYAMLStrUnquoted(t *testing.T) {
	t.Parallel()
	var yt Container
	err := yaml.Unmarshal([]byte(`count: 8M`), &yt)
	Assert(t).IsNil(err, "unexpected unmarshal error")
	Assert(t).AreEqual(ByteCount(8388608), yt.Count, "error parsing unquoted string")
}

// For compatibility with old config formats, the output should be parsable as an int
func TestYAMLCompat(t *testing.T) {
	t.Parallel()
	var yt Container
	var it struct {
		Count int
	}
	yt.Count = ByteCount(1024)
	data, err := yaml.Marshal(yt)
	Assert(t).IsNil(err, "unexpected marshal error")
	t.Logf("`%#v` marshals into: `%s`", yt, string(data))
	err = yaml.Unmarshal(data, &it)
	Assert(t).IsNil(err, "could not unmarshal into int")
	Assert(t).AreEqual(1024, it.Count, "bad value unmarshaling into int")
}

// Should raise an error if the parse fails
func TestYAMLErr(t *testing.T) {
	t.Parallel()
	var yt Container
	err := yaml.Unmarshal([]byte(`count: not a number`), &yt)
	Assert(t).IsNotNil(err, "parse should not have succeeded")
}

func TestYAMLConsistency(t *testing.T) {
	t.Parallel()
	values := []string{
		`10`,
		`4K`,
		`"10000"`,
		`"2000G"`,
	}
	for _, value := range values {
		t.Log("Trying value: ", value)
		var count ByteCount
		err := yaml.Unmarshal([]byte(value), &count)
		Assert(t).IsNil(err, "unexpected unmarshal error")
		data, err := yaml.Marshal(count)
		Assert(t).IsNil(err, "unexpected marshal error")
		data2, err := yaml.Marshal(&count)
		Assert(t).IsNil(err, "unexpected marshal error")
		Assert(t).AreEqual(string(data), string(data2), "value and pointer marshal differently")
		var count2 ByteCount
		err = yaml.Unmarshal(data, &count2)
		Assert(t).IsNil(err, "unexpected unmarshal error")
		Assert(t).AreEqual(count, count2, "value did not survive ")
	}
}
