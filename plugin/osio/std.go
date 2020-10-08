package osio

import (
	"fmt"
	"os"
	"sync"

	"github.com/jbvmio/lfm/plugin"
)

// StdOutputConfig contains configuration details when using the StdOutput Plugin.
type StdOutputConfig struct {
}

// Configure attempts to configure the Config based on the details entered.
func (c *StdOutputConfig) Configure(details map[string]interface{}) error {
	return nil
}

// CreateOutput creates an Input based on the Config.
func (c *StdOutputConfig) CreateOutput() (plugin.Output, error) {
	return &StdOutput{
		data:     make(chan []byte),
		errs:     make(chan error),
		stopChan: make(chan struct{}),
		wg:       sync.WaitGroup{},
	}, nil
}

// StdOutput outputs to stdout.
type StdOutput struct {
	data     chan []byte
	errs     chan error
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// Start starts the plugin.
func (out *StdOutput) Start() error {
	out.wg.Add(1)
	go func() {
		defer out.wg.Done()
	outLoop:
		for {
			select {
			case <-out.stopChan:
				break outLoop
			case input := <-out.data:
				fmt.Fprintf(os.Stdout, "%s\n", input)
			}
		}
	}()
	return nil
}

// Stop stops the plugin.
func (out *StdOutput) Stop() error {
	close(out.stopChan)
	out.wg.Wait()
	return nil
}

// Destination returns the channel used for accept data to the intended Plugin destination.
func (out *StdOutput) Destination() chan<- []byte {
	return out.data
}

// Errors returns the error channel for the Input Plugin.
func (out *StdOutput) Errors() <-chan error {
	return out.errs
}
