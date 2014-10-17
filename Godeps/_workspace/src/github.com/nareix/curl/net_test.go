
package curl

import (
	"testing"
	"fmt"
)

func TestOptDur(t *testing.T) {
	opts := []interface{} {
		"timeout=", 10,
	}
	has, dur := optDuration("timeout=", opts)
	if !has {
		t.Fail()
	}
	fmt.Println(dur)
}

