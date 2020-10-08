package lfm

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

// Configs contain multiple configurations.
type Configs map[string]Config

// Config details for lfm.
type Config struct {
	Sources      []map[string]interface{} `yaml:"sources"`
	Destinations []map[string]interface{} `yaml:"destinations"`
	Processors   []Stage                  `yaml:"processors"`
}

// Stage holds the stage order and Step definitions.
type Stage struct {
	Stage int    `yaml:"stage"`
	Steps []Step `yaml:"steps"`
}

// Step holds the processing instructions.
type Step struct {
	Step     int                    `yaml:"step"`
	Workflow map[string]interface{} `yaml:"workflow"`
	//Workflow []map[string]interface{} `yaml:"workflow"`
}

// ConfigFromFile loads and returns Configs from a local file.
func ConfigFromFile(path string) (cfgs Configs, err error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return Configs{}, err
	}
	err = yaml.Unmarshal(b, &cfgs)
	return
}

func loadFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}
