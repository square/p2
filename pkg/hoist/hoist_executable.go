package hoist

import (
	"fmt"
	"io"
	"strings"

	"github.com/square/p2/pkg/runit"
)

type HoistExecutable struct {
	Service       runit.Service
	ExecPath      string
	Chpst         string
	Contain       string
	Container     string
	ContainerName string
	Nolimit       string
	RunAs         string
	ConfigDir     string
}

func (e HoistExecutable) SBEntry() []string {
	var ret []string
	ret = append(ret, e.Nolimit)
	if e.Container != "" {
		ret = append(ret,
			e.Contain,
			"-a",
			e.ContainerName,
			"-s",
			e.Container,
			"-v",
			"--",
		)
	}
	ret = append(ret,
		e.Chpst,
		"-u",
		strings.Join([]string{e.RunAs, e.RunAs}, ":"),
		"-e",
		e.ConfigDir,
	)
	ret = append(ret, e.ExecPath)
	return ret
}

func (e HoistExecutable) WriteExecutor(writer io.Writer) error {
	_, err := io.WriteString(writer, fmt.Sprintf(`#!/bin/sh
    exec %s
    `, strings.Join(e.SBEntry(), " ")))
	return err
}
