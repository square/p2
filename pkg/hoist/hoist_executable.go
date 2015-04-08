package hoist

import (
	"fmt"
	"io"
	"strings"

	"github.com/square/p2/pkg/runit"
)

type Executable struct {
	Service runit.Service
	Exec    []string
}

func (e Executable) WriteExecutor(writer io.Writer) error {
	_, err := io.WriteString(writer, fmt.Sprintf(`#!/bin/sh
exec %s
`, strings.Join(e.Exec, " ")))
	return err
}
