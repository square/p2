/*
Copyright 2014 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package labels

import (
	"regexp"
	"strings"
)

const qnameCharFmt string = "[A-Za-z0-9]"
const qnameExtCharFmt string = "[-A-Za-z0-9_.]"
const qualifiedNameFmt string = "(" + qnameCharFmt + qnameExtCharFmt + "*)?" + qnameCharFmt
const qualifiedNameMaxLength int = 63

var qualifiedNameRegexp = regexp.MustCompile("^" + qualifiedNameFmt + "$")

func isQualifiedName(value string) bool {
	parts := strings.Split(value, "/")
	var name string
	switch len(parts) {
	case 1:
		name = parts[0]
	case 2:
		var prefix string
		prefix, name = parts[0], parts[1]
		if prefix == "" || !isDNS1123Subdomain(prefix) {
			return false
		}
	default:
		return false
	}

	return name != "" && len(name) <= qualifiedNameMaxLength && qualifiedNameRegexp.MatchString(name)
}

const labelValueFmt string = "(" + qualifiedNameFmt + ")?"
const labelValueMaxLength int = 63

var labelValueRegexp = regexp.MustCompile("^" + labelValueFmt + "$")

func isValidLabelValue(value string) bool {
	return (len(value) <= labelValueMaxLength && labelValueRegexp.MatchString(value))
}

const dns1123LabelFmt string = "[a-z0-9]([-a-z0-9]*[a-z0-9])?"
const dns1123SubdomainFmt string = dns1123LabelFmt + "(\\." + dns1123LabelFmt + ")*"
const dns1123SubdomainMaxLength int = 253

var dns1123SubdomainRegexp = regexp.MustCompile("^" + dns1123SubdomainFmt + "$")

// isDNS1123Subdomain tests for a string that conforms to the definition of a
// subdomain in DNS (RFC 1123).
func isDNS1123Subdomain(value string) bool {
	return len(value) <= dns1123SubdomainMaxLength && dns1123SubdomainRegexp.MatchString(value)
}
