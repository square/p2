package randseed

import (
	crand "crypto/rand"
	"encoding/binary"
	mrand "math/rand"
	"time"
)

// Get returns a value intended to be used as a seed for the random number generators in
// the "math/rand" package.
func Get() int64 {
	var seed int64
	if err := binary.Read(crand.Reader, binary.LittleEndian, &seed); err == nil {
		return seed
	}
	return time.Now().Unix()
}

// NewRand returns a PRNG using a new entropy source.
func NewRand() *mrand.Rand {
	return mrand.New(mrand.NewSource(Get()))
}
