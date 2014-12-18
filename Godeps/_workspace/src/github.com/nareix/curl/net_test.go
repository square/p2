package curl

import (
	"fmt"
	"testing"
)

func TestOptDur(t *testing.T) {
	opts := []interface{}{
		"timeout=", 10,
	}
	has, dur := optDuration("timeout=", opts)
	if !has {
		t.Fail()
	}
	fmt.Println(dur)
}
