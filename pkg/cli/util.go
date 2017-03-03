// package cli provides functionality that may be useful in multiple CLIs
package cli

import (
	"fmt"
	"strings"
)

func Confirm() bool {
	fmt.Printf(`Type "y" to confirm [n]: `)
	var input string
	_, err := fmt.Scanln(&input)
	if err != nil {
		return false
	}
	resp := strings.TrimSpace(strings.ToLower(input))
	return resp == "y" || resp == "yes"
}
