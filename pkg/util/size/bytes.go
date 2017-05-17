// Package size provides a converter between a string representation of a size and
// a number of bytes, and reverse.
package size

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type ByteCount float64

const (
	Byte ByteCount = 1 << (10 * iota)
	Kibibyte
	Mebibyte
	Gibibyte
	Tebibyte
)

var byteStringPattern = regexp.MustCompile(`(?i)^\s*(\-?[\d\.]+)\s*([TGMK]b?|B|)\s*$`)

func (b ByteCount) String() string {
	var ret string
	switch {
	case b >= Tebibyte:
		ret = fmt.Sprintf("%7.1fT", b/Tebibyte)
	case b >= Gibibyte:
		ret = fmt.Sprintf("%7.1fG", b/Gibibyte)
	case b >= Mebibyte:
		ret = fmt.Sprintf("%7.1fM", b/Mebibyte)
	case b >= Kibibyte:
		ret = fmt.Sprintf("%7.1fK", b/Kibibyte)
	default:
		return fmt.Sprintf("%dB", int64(b))
	}
	return strings.TrimSpace(ret)
}

// Parse a string containing a string representation of a byte count.
func Parse(sizeStr string) (ByteCount, error) {
	if !byteStringPattern.MatchString(sizeStr) {
		return 0, fmt.Errorf("Invalid byte representation provided: %s", sizeStr)
	}

	subs := byteStringPattern.FindStringSubmatch(sizeStr)

	size, err := strconv.ParseFloat(string(subs[1]), 64)
	if err != nil {
		return 0, fmt.Errorf("Invalid byte representation %s provided, got error: %s", sizeStr, err)
	}

	unit := strings.ToUpper(string(subs[2]))

	switch unit {
	case "B", "":
		size = size * float64(Byte)
	case "KB", "K":
		size = size * float64(Kibibyte)
	case "MB", "M":
		size = size * float64(Mebibyte)
	case "GB", "G":
		size = size * float64(Gibibyte)
	case "TB", "T":
		size = size * float64(Tebibyte)
	}

	return ByteCount(size), nil
}

// MarshalYAML serializes a ByteCount into YAML. Prefer encoding as a human-readable
// string. If that encoding is lossy, a raw integer will be used.
func (b ByteCount) MarshalYAML() (interface{}, error) {
	s := b.String()
	b2, err := Parse(s)
	if err != nil || uint64(b) != uint64(b2) {
		return uint64(b), nil
	}
	return s, nil
}

// UnmarshalYAML unserializes a YAML representation of the ByteCount.
func (b *ByteCount) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var strVal string
	err := unmarshal(&strVal)
	if err != nil {
		return err
	}
	parsed, err := Parse(strVal)
	if err != nil {
		return err
	}
	*b = parsed
	return nil
}
