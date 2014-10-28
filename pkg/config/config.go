// Package config provides convenience facilities for Golang-based pods to read their
// configuration files provided either by the environment or a custom path.
package config

import (
	"errors"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	unpacked map[interface{}]interface{}
}

func LoadFromEnvironment() (*Config, error) {
	env := os.Getenv("CONFIG_PATH")
	if env == "" {
		return nil, errors.New("No value was found for the environment variable CONFIG_PATH")
	}
	return LoadConfigFile(env)
}

func LoadConfigFile(filepath string) (*Config, error) {
	config := &Config{}
	contents, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	yaml.Unmarshal(contents, &config.unpacked)
	return config, nil
}

func (c *Config) ReadString(key string) string {
	readVal := c.Read(key)
	if readVal == nil {
		return ""
	}
	return readVal.(string)
}

func (c *Config) Read(key string) interface{} {
	return c.unpacked[key]
}
