package artifact

import (
	"regexp"
)

var (
	// similar to https://github.com/appc/spec/blob/master/spec/types.md#ac-identifier-type but
	// adds a simple tag facility, removes home directories, and limits subpath depth to 1
	imageSpec = regexp.MustCompile(`^[a-z0-9]+([-._/][a-z0-9]+)(:[a-z0-9]+)?$`)
)
