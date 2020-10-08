package drivers

import (
	"fmt"
	"io/ioutil"

	"github.com/jbvmio/lfm/driver"

	"github.com/jbvmio/lfm/pipeline"
)

// MakeDriversFunc creates a useable function using the given Drivers.
func MakeDriversFunc(steps [][]driver.Driver) func(pipeline.Data) (bool, error) {
	if len(steps) < 1 {
		return pipeline.NoopData
	}
	return func(d pipeline.Data) (bool, error) {
		if len(d.Bytes()) < 1 {
			return false, fmt.Errorf("empty data received")
		}
		data, err := ioutil.ReadAll(d)
		if err != nil {
			return false, fmt.Errorf("could not read data: %w", err)
		}
		P := driver.NewPayload()
		defer P.Discard()
		for _, drivers := range steps {
			for _, d := range drivers {
				P.UseBytes(data)
				go d.Process(P)
				result := <-P.Results()
				if result.Error() != nil {
					return false, result.Error()
				}
				if len(result.Bytes()) < 1 {
					return false, nil
				}
				data = result.Bytes()
			}
		}
		d.Write(data)
		return true, nil
	}
}
