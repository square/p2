package podprocess

func (p *Reporter) FinishExec() []string {
	return []string{
		"/usr/bin/timeout",
		"10",
		p.environmentExtractorPath,
		"$1",
		"$2",
		"|",
		"base64",
		"-w0",
		"|",
		"nc",
		"-U",
		p.sockPath,
	}
}
