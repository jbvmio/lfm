package json

import (
	JS "encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/jbvmio/lfm/driver"
	"github.com/tidwall/gjson"
)

const (
	funcExpr = `([a-zA-Z]+)\((.*)\)`
)

// Payload is a driver Payload with JSON Driver Specifics.
type Payload interface {
	driver.Payload
	SetRemove(bool)
	Remove() bool
}

type jsonPayload struct {
	driver.Payload
	remove bool
	err    error
}

// NewJSONPayload converts and returns a Payload from a Payload.
func NewJSONPayload(P driver.Payload) Payload {
	return &jsonPayload{P, false, nil}
}

func (p *jsonPayload) Remove() bool {
	return p.remove
}

func (p *jsonPayload) SetRemove(x bool) {
	p.remove = x
}

var funcRegex = regexp.MustCompile(funcExpr)

type jsonMethodFn func([]byte) interface{}

type jsonConditionalFn func(interface{}) bool

type jsonMakeConditionalFn func(string) jsonConditionalFn

type jsonMakeMethodFn func(string) jsonMethodFn

type jsonMakeActionFn func(string, jsonMethodFn, ...jsonConditionalFn) func(P Payload)

var jsonMethodFunc = map[string]jsonMakeMethodFn{
	`extract`:   jsonMakeExtractMethodFunc,
	`filter`:    jsonMakeTransformMethodFunc,
	`transform`: jsonMakeTransformMethodFunc,
}

var jsonMethodAllowedActions = map[string]map[string]jsonMakeActionFn{
	`extract`: {
		`addField`: jsonMakeAddFieldFunc,
		`addTag`:   jsonMakeAddTagFunc,
		`addVar`:   jsonMakeAddVarFunc,
	},
	`filter`: {
		`remove`: jsonMakeRemoveFunc,
		`keep`:   jsonMakeKeepFunc,
	},
	`transform`: {
		`drop`:        jsonMakeDropFieldFunc,
		`changeField`: jsonMakeChangeFieldFunc,
		`changeValue`: jsonMakeChangeValueFunc,
	},
}

var jsonActionAllowedConditions = map[string]map[string]jsonMakeConditionalFn{
	`addField`: {
		`containsString`: jsonMakeConditionalContainsString,
		`matchString`:    jsonMakeConditionalMatchString,
		`matchRegex`:     jsonMakeConditionalMatchRegex,
	},
	`addTag`: {
		`containsString`: jsonMakeConditionalContainsString,
		`matchString`:    jsonMakeConditionalMatchString,
		`matchRegex`:     jsonMakeConditionalMatchRegex,
	},
	`addVar`: {
		`containsString`: jsonMakeConditionalContainsString,
		`matchString`:    jsonMakeConditionalMatchString,
		`matchRegex`:     jsonMakeConditionalMatchRegex,
	},
	`remove`: {
		`containsString`: jsonMakeConditionalContainsString,
		`matchString`:    jsonMakeConditionalMatchString,
		`matchRegex`:     jsonMakeConditionalMatchRegex,
	},
	`keep`: {
		`containsString`: jsonMakeConditionalContainsString,
		`matchString`:    jsonMakeConditionalMatchString,
		`matchRegex`:     jsonMakeConditionalMatchRegex,
	},
	`drop`: {
		`containsString`: jsonMakeConditionalContainsString,
		`matchString`:    jsonMakeConditionalMatchString,
		`matchRegex`:     jsonMakeConditionalMatchRegex,
	},
	`changeField`: {
		`containsString`: jsonMakeConditionalContainsString,
		`matchString`:    jsonMakeConditionalMatchString,
		`matchRegex`:     jsonMakeConditionalMatchRegex,
	},
	`changeValue`: {
		`containsString`: jsonMakeConditionalContainsString,
		`matchString`:    jsonMakeConditionalMatchString,
		`matchRegex`:     jsonMakeConditionalMatchRegex,
	},
}

/*
var jsonMethodAllowedConditions = map[string]map[string]jsonMakeConditionalFn{
	`extract`: {
		`containsString`: jsonMakeConditionalContainsString,
		`matchString`:    jsonMakeConditionalMatchString,
		`matchRegex`:     jsonMakeConditionalMatchRegex,
	},
	`filter`: {
		`containsString`: jsonMakeConditionalContainsString,
		`matchString`:    jsonMakeConditionalMatchString,
		`matchRegex`:     jsonMakeConditionalMatchRegex,
	},
	`transform`: {
		`containsString`: jsonMakeConditionalContainsString,
		`matchString`:    jsonMakeConditionalMatchString,
		`matchRegex`:     jsonMakeConditionalMatchRegex,
	},
}
*/

var jsonNoopPathActionFunc = func(P Payload) {

}

var jsonNoopConditionFunc = func(x interface{}) bool {
	return true
}

var jsonDriverActionKV = map[string]string{
	`getVar`: driver.VarsLabel,
	`getTag`: driver.TagsLabel,
}

func jsonMakeNoopPathActionFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(Payload) {
	return jsonNoopPathActionFunc
}

func jsonMakeAddFieldFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	kv := strings.Split(name, `,`)
	switch len(kv) {
	case 2:
		return func(P Payload) {
			val := fn(P.Bytes())
			if val == nil {
				return
			}
			for _, c := range cs {
				if c(val) {
					P.KV(JSONFieldsLabel).Add(kv[0], kv[1])
					return
				}
			}
		}
	default:
		return func(P Payload) {
			val := fn(P.Bytes())
			if val == nil {
				return
			}
			for _, c := range cs {
				if c(val) {
					P.KV(JSONFieldsLabel).Add(name, val)
					return
				}
			}
		}
	}
}

func jsonMakeAddTagFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	return func(P Payload) {
		val := fn(P.Bytes())
		if val == nil {
			return
		}
		for _, c := range cs {
			if c(val) {
				P.KV(driver.TagsLabel).Add(name, val)
				return
			}
		}
	}
}

func jsonMakeAddVarFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	return func(P Payload) {
		val := fn(P.Bytes())
		if val == nil {
			return
		}
		for _, c := range cs {
			if c(val) {
				P.KV(driver.VarsLabel).Add(name, val)
				return
			}
		}
	}
}

func jsonMakeExtractMethodFunc(path string) jsonMethodFn {
	switch path {
	case `.`:
		return func(data []byte) interface{} {
			r := gjson.ParseBytes(data)
			switch {
			case r.IsObject():
				return r.Value()
			default:
				var x interface{}
				if err := JS.Unmarshal(data, &x); err != nil {
					return fmt.Sprintf("%s", data)
				}
				return x
			}
		}
	default:
		return func(data []byte) interface{} {
			r := gjson.ParseBytes(data).Get(path)
			return r.Value()
		}
	}
}

func jsonMakeFilterMethodFunc(path string) jsonMethodFn {
	return func(data []byte) interface{} {
		r := gjson.ParseBytes(data).Get(path)
		return r.Value()
	}
}

func jsonMakeTransformMethodFunc(path string) jsonMethodFn {
	return func(data []byte) interface{} {
		d := make([]interface{}, 2)
		d[0] = gjson.ParseBytes(data).Value()
		d[1] = gjson.ParseBytes(data).Get(path).Value()
		return d
	}
}

func jsonMakeDropFieldFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	return func(P Payload) {

		var b []byte
		fields := P.KV(JSONFieldsLabel).All()
		switch len(fields) {
		case 0:
			b = P.Bytes()
		default:
			var err error
			b, err = JS.Marshal(fields)
			if err != nil {
				P.UseError(fmt.Errorf("processing change field on existing data: %w", err))
				return
			}
		}
		val := fn(b)
		if val == nil {
			P.UseError(fmt.Errorf("received nil value from method"))
			return
		}
		v := val.([]interface{})[0]
		var targetVal interface{}
		switch obj := v.(type) {
		case map[string]interface{}:
			obj = fields
			levels := parseLevels(name)
			var lastKey string
			switch len(levels) {
			case 1:
				lastKey = levels[0]
			default:
				lastKey = levels[len(levels)-1]
				for _, val := range levels[:len(levels)-1] {
					if _, ok := obj[val].(map[string]interface{}); ok {
						obj = obj[val].(map[string]interface{})
					} else {
						P.UseError(fmt.Errorf("jsonpath is invalid: %s", name))
					}
				}
			}
			targetVal = obj[lastKey]
			delete(obj, lastKey)
		}
		for _, c := range cs {
			if c(targetVal) {
				P.KV(JSONFieldsLabel).Use(fields)
				return
			}
		}
	}
}

func jsonMakeChangeFieldFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	return func(P Payload) {
		var b []byte
		fields := P.KV(JSONFieldsLabel).All()
		switch len(fields) {
		case 0:
			b = P.Bytes()
		default:
			var err error
			b, err = JS.Marshal(fields)
			if err != nil {
				P.UseError(fmt.Errorf("processing change field on existing data: %w", err))
				return
			}
		}
		val := fn(b)
		if val == nil {
			P.UseError(fmt.Errorf("received nil value from method"))
			return
		}
		v := val.([]interface{})[0]
		target := val.([]interface{})[1]
		var targetVal interface{}
		switch obj := v.(type) {
		case map[string]interface{}:
			obj = fields
			levels := parseLevels(name)
			for _, val := range levels[:len(levels)-1] {
				obj = obj[val].(map[string]interface{})
			}
			for k, v := range obj {
				if v == target {
					targetVal = v
					obj[name] = target
					delete(obj, k)
					break
				}
			}
		}
		for _, c := range cs {
			if c(targetVal) {
				P.KV(JSONFieldsLabel).Use(fields)
				return
			}
		}
	}
}

func jsonMakeChangeValueFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	return func(P Payload) {

		var b []byte
		fields := P.KV(JSONFieldsLabel).All()
		switch len(fields) {
		case 0:
			b = P.Bytes()
		default:
			var err error
			b, err = JS.Marshal(fields)
			if err != nil {
				P.UseError(fmt.Errorf("processing change field on existing data: %w", err))
				return
			}
		}
		val := fn(b)
		if val == nil {
			P.UseError(fmt.Errorf("received nil value from method"))
			return
		}
		v := val.([]interface{})[0]
		target := val.([]interface{})[1]
		var targetVal interface{}
		switch obj := v.(type) {
		case map[string]interface{}:
			obj = fields
			levels := parseLevels(name)
			for _, val := range levels[:len(levels)-1] {
				obj = obj[val].(map[string]interface{})
			}
			for k, v := range obj {
				if v == target {
					targetVal = v
					obj[k] = name
					break
				}
			}
		default:
			fmt.Printf(">> ChangeValue of Type: %T\n", obj)
		}
		for _, c := range cs {
			if c(targetVal) {
				P.KV(JSONFieldsLabel).Use(fields)
				return
			}
		}
	}
}

func jsonMakeRemoveFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	return func(P Payload) {
		val := fn(P.Bytes())
		if val == nil {
			return
		}
		v := val.([]interface{})[1]
		for _, c := range cs {
			if c(v) {
				P.SetRemove(true)
				return
			}
		}
	}
}

func jsonMakeKeepFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	return func(P Payload) {
		val := fn(P.Bytes())
		if val == nil {
			return
		}
		v := val.([]interface{})[1]
		for _, c := range cs {
			if c(v) {
				return
			}
		}
		P.SetRemove(true)
	}
}

func jsonMakeConditionalMatchString(arg string) jsonConditionalFn {
	return func(x interface{}) bool {
		if val, ok := x.(string); ok {
			if val == arg {
				return true
			}
		}
		return false
	}
}

func jsonMakeConditionalContainsString(arg string) jsonConditionalFn {
	return func(x interface{}) bool {
		if val, ok := x.(string); ok {
			if strings.Contains(val, arg) {
				return true
			}
		}
		return false
	}
}

func jsonMakeConditionalMatchRegex(arg string) jsonConditionalFn {
	regex, err := regexp.Compile(arg)
	if err != nil {
		return func(x interface{}) bool {
			return false
		}
	}
	return func(x interface{}) bool {
		val := fmt.Sprintf("%v", x)
		if regex.MatchString(val) {
			return true
		}
		return false
	}
}

/*
func jsonMakeExistsFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	return func(P Payload) {
		val := fn(P.Bytes())
		if val == nil {
			return
		}
		v := val.([]interface{})[1]
		if v != nil {
			P.SetMatch(true)
		} else {
			P.SetNoMatch(true)
		}
	}
}



func jsonMakeMatchStringFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	return func(P Payload) {
		val := fn(P.Bytes())
		if val == nil {
			return
		}
		v := val.([]interface{})[1]
		target, ok := v.(string)
		switch {
		case ok:
			if target == name {
				P.SetMatch(true)
			} else {
				P.SetNoMatch(true)
			}
		default:
			P.SetNoMatch(true)
		}
	}
}

func jsonMakeContainsStringFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	return func(P Payload) {
		val := fn(P.Bytes())
		if val == nil {
			return
		}
		v := val.([]interface{})[1]
		target, ok := v.(string)
		switch {
		case ok:
			if strings.Contains(target, name) {
				P.SetMatch(true)
			} else {
				P.SetNoMatch(true)
			}
		default:
			P.SetNoMatch(true)
		}
	}
}

func jsonMakeMatchRegexFunc(name string, fn jsonMethodFn, cs ...jsonConditionalFn) func(P Payload) {
	regex, err := regexp.Compile(name)
	if err != nil {
		return func(P Payload) {
			P.UseError(fmt.Errorf("invalid regex expression: %w", err))
			P.SetNoMatch(true)
		}
	}
	return func(P Payload) {
		val := fn(P.Bytes())
		if val == nil {
			return
		}
		v := val.([]interface{})[1]
		target, ok := v.(string)
		switch {
		case ok:
			if regex.MatchString(target) {
				P.SetMatch(true)
			} else {
				P.SetNoMatch(true)
			}
		default:
			P.SetNoMatch(true)
		}
	}
}
*/

func parseFunction(f string) (valid bool, fn, arg string) {
	x := funcRegex.FindStringSubmatch(f)
	switch len(x) {
	case 2:
		switch x[1] {
		case `drop`, `keep`, `remove`:
			fn = x[1]
			valid = true
		case `exists`:
			fn = x[1]
			valid = true
		}
	case 3:
		fn = x[1]
		arg = x[2]
		valid = true
	}
	return
}

func parseLevels(path string) (levels []string) {
	l := strings.Split(path, `.`)
	skip := make(map[int]bool)
	for n, lev := range l {
		switch {
		case skip[n]:
		case strings.HasSuffix(lev, `\`):
			levels = append(levels, strings.Replace(lev+`.`+l[n+1], `\`, ``, 1))
			skip[n+1] = true
		default:
			levels = append(levels, lev)
		}
	}
	return
}

func jsonConvertToObject(b []byte) map[string]interface{} {
	val := gjson.ParseBytes(b).Value()
	if v, ok := val.(map[string]interface{}); ok {
		return v
	}
	return nil
}