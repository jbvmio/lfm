package config

import (
	"github.com/jbvmio/lfm/plugin"
	"github.com/jbvmio/lfm/plugin/kafka"
	"github.com/jbvmio/lfm/plugin/loki"
	"github.com/jbvmio/lfm/plugin/osio"
)

// Config represents configuration details for a Input or Output Plugin.
type Config interface {
	Configure(map[string]interface{}) error
}

// InputConfig is a Config for an Input Plugin.
type InputConfig interface {
	Config
	CreateInput() (plugin.Input, error)
}

// OutputConfig is a Config for an Output Plugin.
type OutputConfig interface {
	Config
	CreateOutput() (plugin.Output, error)
}

// GetInputConfig returns an InputConfig based on the entered ID.
// Returns nil if TypeID is None an invalid ID is entered.
func GetInputConfig(i plugin.TypeID) InputConfig {
	switch i {
	case plugin.TypeNone:
		return nil
	case plugin.TypeInputFile:
		return &osio.FileInputConfig{}
	case plugin.TypeInputKafka:
		return &kafka.InputConfig{}
	default:
		return nil
	}
}

// GetOutputConfig returns an Output based on the entered ID.
// Returns nil if TypeID is None an invalid ID is entered.
func GetOutputConfig(i plugin.TypeID) OutputConfig {
	switch i {
	case plugin.TypeNone:
		return nil
	case plugin.TypeOutputFile:
		return &osio.FileOutputConfig{}
	case plugin.TypeOutputKafka:
		return &kafka.OutputConfig{}
	case plugin.TypeOutputLoki:
		return &loki.OutputConfig{}
	case plugin.TypeOutputStd:
		return &osio.StdOutputConfig{}
	default:
		return nil
	}
}
