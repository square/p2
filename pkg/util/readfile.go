package util

import (
	"io/ioutil"
	"strings"
)

func LoadTokens(path string) ([]string, error) {
	tokens, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	return strings.Split(string(tokens), "\n"), nil
}
