package util

import (
	"path"
)

type Caller struct {
	Filename string
}

// Get a caller object. This is intented to be passed runtime.Caller()
// from the target
func From(pc uintptr, file string, line int, ok bool) *Caller {
	if !ok {
		return nil
	}
	return &Caller{file}
}

func (c *Caller) ExpandPath(pathstr string) string {
	return path.Join(path.Dir(c.Filename), pathstr)
}
