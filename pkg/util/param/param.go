// The param package is a management system for configuration parameters. Much like the
// standard library "flags" package, the param package allows parameters to be defined in
// the file they are needed without requiring direct linkage between the usage location
// and the configuration parser.
//
// In typical use, each parameter is defined as a package-level variable. During
// initialization, the program provides a map[string]string containing parameter
// configuration data. This map is expected to be parsed from a configuration file.
//
// Because the tasks are so similar, this package is implemented by re-using the low-level
// types from the standard "flag" module.
//
// Example parameter declaration:
//
//   var threshhold = param.Int("threshhold", 5)
//
// Parameter use:
//
//   if numUsed >= *threshhold { ... }
//
// Parsing from a config file:
//
//   var config struct {
//       FormalValue  string
//       Params       param.Values
//   }
//   yaml.Unmarshal(readConfigFile(), &config)
//   err := param.Parse(config.Params)
//
package param

import (
	"flag"
	"fmt"
)

// DefaultParams holds the default set of parameters, used by all package-level functions.
var DefaultParams flag.FlagSet

// Values is a type alias that can be used to help document the configuration values that
// are meant to be define parameters.
type Values map[string]string

// Float64 declares a new "float64"-typed parameter with the given default value.
func Float64(name string, value float64) *float64 {
	return DefaultParams.Float64(name, value, "")
}

// Int declares a new "int"-typed parameter with the given default value.
func Int(name string, value int) *int {
	return DefaultParams.Int(name, value, "")
}

// Int64 declares a new "int64"-typed parameter with the given default value.
func Int64(name string, value int64) *int64 {
	return DefaultParams.Int64(name, value, "")
}

// String declares a new "string"-typed parameter with the given default value.
func String(name string, value string) *string {
	return DefaultParams.String(name, value, "")
}

// Parse parses the parameter assignments for the default set of parameters. See
// ParseFlags().
func Parse(values Values) error {
	return ParseFlags(&DefaultParams, values)
}

// ParseFlags parses a map of parameter assignments that are defined by a FlagSet.
func ParseFlags(f *flag.FlagSet, values Values) error {
	for name := range values {
		if f.Lookup(name) == nil {
			return fmt.Errorf("%s: no such parameter", name)
		}
	}
	var err error
	for name, value := range values {
		err = f.Set(name, value)
		if err != nil {
			err = fmt.Errorf("%s: %s", name, err)
		}
	}
	return err
}
