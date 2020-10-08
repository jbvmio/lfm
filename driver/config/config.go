package config

import (
	"github.com/jbvmio/lfm/driver"
	"github.com/jbvmio/lfm/driver/json"
	"github.com/pkg/errors"
)

// Config represents configuration details for a Driver.
type Config interface {
	Configure(map[string]interface{}) error
}

// FromConfig attempts to Generate the appropriate Driver based on the details entered.
func FromConfig(details map[string]interface{}) (driver.Driver, error) {
	cfg, err := GetConfig(details)
	if err != nil {
		return nil, err
	}
	switch C := cfg.(type) {
	case *json.Config:
		return json.NewDriver(C)
	default:
		return nil, errors.Errorf("missing or invalid driver type: %T", cfg)
	}
}

// GetConfig attempts to Generate a Config based on the details entered.
func GetConfig(details map[string]interface{}) (Config, error) {
	var cfg Config
	d, there := details[`driver`].(string)
	switch {
	case !there || d == "":
		return nil, errors.New("missing or invalid driver")
	case d == `json`:
		C := &json.Config{}
		err := C.Configure(details)
		if err != nil {
			return nil, err
		}
		cfg = C
	default:
		return nil, errors.Errorf("invalid driver: %s", d)
	}
	return cfg, nil
}
