package plugins

import (
	"fmt"

	"github.com/jbvmio/lfm"
	"github.com/jbvmio/lfm/plugin"
	"github.com/jbvmio/lfm/plugin/config"
)

// LoadInputs loads Input Plugins.
func LoadInputs(cfg lfm.Configs) (inputs map[string][]plugin.Input, err error) {
	inputs = make(map[string][]plugin.Input)
	if len(cfg) < 1 {
		return nil, fmt.Errorf("invalid config")
	}
	for k, v := range cfg {
		var ins []plugin.Input
		for _, input := range v.Sources {
			p, err := pluginName(input)
			if err != nil {
				return nil, fmt.Errorf("error loading input for %s: %v", k, err)
			}
			in, err := loadInputPlugin(k, p, input)
			if err != nil {
				return nil, fmt.Errorf("error loading input: %v", err)
			}
			ins = append(ins, in)
		}
		inputs[k] = append(inputs[k], ins...)
	}
	return
}

// LoadOutputs loads Output Plugins.
func LoadOutputs(cfg lfm.Configs) (outputs map[string][]plugin.Output, err error) {
	outputs = make(map[string][]plugin.Output)
	if len(cfg) < 1 {
		return nil, fmt.Errorf("invalid config")
	}
	for k, v := range cfg {
		var outs []plugin.Output
		for _, output := range v.Destinations {
			p, err := pluginName(output)
			if err != nil {
				return nil, fmt.Errorf("error loading output for %s: %v", k, err)
			}
			out, err := loadOutputPlugin(p, output)
			if err != nil {
				return nil, fmt.Errorf("error loading output: %v", err)
			}
			outs = append(outs, out)
		}
		outputs[k] = append(outputs[k], outs...)
	}
	return
}

func loadInputPlugin(id, name string, details map[string]interface{}) (p plugin.Input, err error) {
	var c config.InputConfig
	switch name {
	case `file`:
		c = config.GetInputConfig(plugin.TypeInputFile)
	case `kafka`:
		c = config.GetInputConfig(plugin.TypeInputKafka)
		if g, there := details[`group`].(string); there {
			details[`group`] = g + `-` + id
		}
	default:
		return nil, fmt.Errorf("no defined input plugin named %s available", name)
	}
	if c == nil {
		return nil, fmt.Errorf("invalid plugin %s entered", name)
	}
	err = c.Configure(details)
	if err != nil {
		return nil, fmt.Errorf("error configuring input: %v", err)
	}
	return c.CreateInput()
}

func loadOutputPlugin(name string, details map[string]interface{}) (p plugin.Output, err error) {
	var c config.OutputConfig
	switch name {
	case `file`:
		c = config.GetOutputConfig(plugin.TypeOutputFile)
	case `stdout`:
		c = config.GetOutputConfig(plugin.TypeOutputStd)
	case `loki`:
		c = config.GetOutputConfig(plugin.TypeOutputLoki)
	case `kafka`:
		c = config.GetOutputConfig(plugin.TypeOutputKafka)
	default:
		return nil, fmt.Errorf("no defined output plugin named %s available", name)
	}
	if c == nil {
		return nil, fmt.Errorf("invalid plugin %s entered", name)
	}
	err = c.Configure(details)
	if err != nil {
		return nil, fmt.Errorf("error configuring output: %v", err)
	}
	return c.CreateOutput()
}

func pluginName(x map[string]interface{}) (string, error) {
	p, ok := x[`plugin`].(string)
	if !ok {
		return "", fmt.Errorf("no plugin defined")
	}
	return p, nil
}
