package json

import (
	JS "encoding/json"
	"fmt"

	"github.com/jbvmio/lfm/driver"
	"gopkg.in/yaml.v2"
)

// JSON Driver Constants
const (
	FieldsLabel = `fields`
)

// Config contains configuration details when using the JSON Driver.
type Config struct {
	Method  string         `yaml:"method" json:"method"`
	Fields  []fieldActions `yaml:"fieldActions" json:"fieldActions"`
	Actions driverActions  `yaml:"driverActions" json:"driverActions"`
}

type fieldActions struct {
	Path       string   `yaml:"path" json:"path"`
	Action     string   `yaml:"action" json:"action"`
	Conditions []string `yaml:"conditions" json:"conditions"`
}

type driverActions struct {
	AddFields map[string]interface{} `yaml:"addFields" json:"addFields"`
	AddVars   map[string]interface{} `yaml:"addVars" json:"addVars"`
	AddTags   map[string]string      `yaml:"addTags" json:"addTags"`
}

// Configure attempts to configure the Config based on the details entered.
func (c *Config) Configure(details map[string]interface{}) error {
	d, err := yaml.Marshal(details)
	if err != nil {
		return fmt.Errorf("invalid configuration format: %v", err)
	}
	var cfg Config
	err = yaml.Unmarshal(d, &cfg)
	if err != nil {
		return fmt.Errorf("invalid configuration: %v", err)
	}
	*c = cfg
	if c.Method == "" {
		return fmt.Errorf("missing or invalid method for json driver")
	}
	return nil
}

// Driver holds dynamic data as it is passed through various operations.
type Driver struct {
	actions driverActions
	fns     []func(Payload)
}

type jsonPayload2 struct {
	tags    map[string]string
	fields  map[string]interface{}
	tmpVars map[string]interface{}
	noMatch bool
	err     error
}

func jsonProcessPayload(JP Payload) {
	T := JP.KV(driver.TagsLabel).All()
	F := JP.KV(FieldsLabel).All()
	switch {
	case len(F) < 1:
		switch len(T) {
		case 0:
			JP.Results() <- driver.NewResult(JP.Bytes(), nil)
		default:
			F = jsonConvertToObject(JP.Bytes())
			if F == nil {
				JP.UseError(fmt.Errorf("received nil object"))
				JP.Results() <- JP
				return
			}
			F[driver.TagsLabel] = T
			JP.Results() <- driver.NewResult(JS.Marshal(F))
		}
	default:
		if len(T) > 0 {
			F[driver.TagsLabel] = T
		}
		JP.Results() <- driver.NewResult(JS.Marshal(F))
	}
}

func syncTags(P driver.Payload, tags map[string]string) {
	for k, v := range tags {
		P.KV(driver.TagsLabel).Add(k, v)
	}
	all := P.KV(driver.TagsLabel).All()
	for k, v := range all {
		tags[k] = v.(string)
	}
}

func syncVars(P driver.Payload, vars map[string]interface{}) {
	for k, v := range vars {
		P.KV(driver.VarsLabel).Add(k, v)
	}
	all := P.KV(driver.VarsLabel).All()
	for k, v := range all {
		vars[k] = v
	}
}

func syncFields(P driver.Payload, fields map[string]interface{}) {
	for k, v := range fields {
		P.KV(FieldsLabel).Add(k, v)
	}
	all := P.KV(FieldsLabel).All()
	for k, v := range all {
		fields[k] = v
	}
}

// Process takes in data and processes it through the Driver.
func (d *Driver) Process(P driver.Payload) {
	JP := NewJSONPayload(P)
	for _, fn := range d.fns {
		fn(JP)
		if JP.Error() != nil {
			P.Results() <- JP
			return
		}
	}
	if JP.Remove() && len(JP.KV(driver.TagsLabel).All()) < 1 {
		JP.Results() <- driver.NewResult([]byte{}, nil)
		return
	}
	d.addVars(JP)
	d.addFields(JP)
	d.addTags(JP)
	go jsonProcessPayload(JP)
}

// addVars add any vars during the Driver Actions phase.
func (d *Driver) addVars(P driver.Payload) {
	for k, v := range d.actions.AddVars {
		var x interface{}
		switch value := v.(type) {
		case string:
			if there, fn, arg := parseFunction(value); there {
				x = P.KV(jsonDriverActionKV[fn]).Get(arg)
			} else {
				x = v
			}
		default:
			x = v
		}
		if x != nil {
			P.KV(driver.VarsLabel).Add(k, x)
		}
	}
}

// addFields add any fields during the Driver Actions phase.
func (d *Driver) addFields(P driver.Payload) {
	for k, v := range d.actions.AddFields {
		var x interface{}
		switch value := v.(type) {
		case string:
			if there, fn, arg := parseFunction(value); there {
				x = P.KV(jsonDriverActionKV[fn]).Get(arg)
			} else {
				x = v
			}
		case []interface{}:
			var tmp []interface{}
			for i := 0; i < len(value); i++ {
				str, ok := value[i].(string)
				switch {
				case ok:
					if there, fn, arg := parseFunction(str); there {
						tmp = append(tmp, P.KV(jsonDriverActionKV[fn]).Get(arg))
					} else {
						tmp = append(tmp, value[i])
					}
				default:
					tmp = append(tmp, value[i])
				}
			}
			x = tmp
		default:
			x = v
		}
		levels := parseLevels(k)
		switch len(levels) {
		case 1:
			P.KV(FieldsLabel).Add(k, x)
		default:
			raw := P.KV(FieldsLabel).All()
			target := raw
			for _, val := range levels[:len(levels)-1] {
				existing, there := target[val]
				switch {
				case there:
					switch E := existing.(type) {
					case map[string]interface{}:
						target = E
					default:
						target[val] = make(map[string]interface{})
						target = target[val].(map[string]interface{})
					}
				default:
					target[val] = make(map[string]interface{})
					target = target[val].(map[string]interface{})
				}
			}
			target[levels[len(levels)-1]] = x
			P.KV(FieldsLabel).Use(raw)
		}
	}
}

// addTags add any tags during the Driver Actions phase.
func (d *Driver) addTags(P driver.Payload) {
	for k, v := range d.actions.AddTags {
		var V string
		if there, fn, arg := parseFunction(v); there {
			val, ok := P.KV(jsonDriverActionKV[fn]).Get(arg).(string)
			if ok {
				V = val
			}
		} else {
			V = v
		}
		if V != "" {
			P.KV(driver.TagsLabel).Add(k, V)
		}
	}
}

// NewDriver returns a new JSON Driver.
func NewDriver(cfg *Config) (*Driver, error) {
	method := jsonMethodFunc[cfg.Method]
	if method == nil {
		return &Driver{}, fmt.Errorf("invalid method %s", cfg.Method)
	}
	driver := Driver{
		actions: cfg.Actions,
	}
	var fns []func(Payload)
	switch len(cfg.Fields) {
	case 0:
		fns = append(fns, jsonNoopPathActionFunc)
	default:
		for _, f := range cfg.Fields {
			if f.Path == "" {
				return &Driver{}, fmt.Errorf("missing path for method %s", cfg.Method)
			}
			fn := method(f.Path)
			var action, name string
			var ok bool
			switch f.Action {
			case "":
				name = f.Path
				switch cfg.Method {
				case `extract`:
					action = `addField`
				case `filter`:
					action = `keep`
				default:
					return &Driver{}, fmt.Errorf("missing action for json method: %s", cfg.Method)
				}
			default:
				ok, action, name = parseFunction(f.Action)
				if !ok {
					return &Driver{}, fmt.Errorf("invalid action for path %s: %s", f.Path, f.Action)
				}
				if name == "" {
					switch action {
					case `drop`, `exists`, `keep`, `remove`:
						name = f.Path
					default:
						return &Driver{}, fmt.Errorf("missing action value for path %s: %s", f.Path, f.Action)
					}
				}
			}

			fmt.Println(">>     METHOD:", cfg.Method)
			fmt.Println(">>     ACTION:", action)
			fmt.Println(">>       NAME:", name)
			fmt.Println(">> CONDITIONS:", f.Conditions)

			cs := make([]jsonConditionalFn, 0, len(f.Conditions))
			switch len(f.Conditions) {
			case 0:
				cs = []jsonConditionalFn{jsonNoopConditionFunc}
			default:
				for _, c := range f.Conditions {
					valid, condition, arg := parseFunction(c)
					if !valid {
						return &Driver{}, fmt.Errorf("invalid condition for path %s: %s", f.Path, condition)
					}
					if C, ok := jsonActionAllowedConditions[action][condition]; ok {
						fmt.Println("  >> APPENDING CONDITION:", condition)
						cs = append(cs, C(arg))
					} else {
						return &Driver{}, fmt.Errorf("invalid condition for action %s: %s", action, condition)
					}

					fmt.Println("  >> CONDITION:", condition)
					fmt.Println("  >>       ARG:", arg)

				}
			}

			fmt.Println()

			var actFn func(Payload)
			if F, ok := jsonMethodAllowedActions[cfg.Method][action]; ok {
				actFn = F(name, fn, cs...)
			}
			if actFn == nil {
				return &Driver{}, fmt.Errorf("invalid action for path %s: %s", f.Path, action)
			}
			fns = append(fns, actFn)
		}
	}
	driver.fns = fns
	return &driver, nil
}
