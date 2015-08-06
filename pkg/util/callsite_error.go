package util

type CallsiteError interface {
	error
	LineNumber() int
	Filename() string
	Function() string
}
