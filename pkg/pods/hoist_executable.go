package pods

import (
	"fmt"
	"io"
	"strings"

	"github.com/square/p2/pkg/runit"
)

type HoistExecutable struct {
	Service   runit.Service
	ExecPath  string
	Chpst     string
	Nolimit   string
	RunAs     string
	ConfigDir string
}

func (e HoistExecutable) SBEntry() []string {
	return []string{
		e.Nolimit,
		e.Chpst,
		"-u",
		strings.Join([]string{e.RunAs, e.RunAs}, ":"),
		"-e",
		e.ConfigDir,
		e.ExecPath,
	}
}

func (e HoistExecutable) WriteExecutor(writer io.Writer) error {
	_, err := io.WriteString(writer, fmt.Sprintf(`#!/bin/sh
    exec %s
    `, strings.Join(e.SBEntry(), " ")))
	return err
}
