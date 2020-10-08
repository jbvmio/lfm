package osio

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/jbvmio/lfm/plugin"
	"github.com/nxadm/tail"
)

// FileInputConfig contains configuration details when using the FileInput Plugin.
type FileInputConfig struct {
	Path           string `yaml:"path" json:"path"`
	Buffer         int    `yaml:"buffer" json:"buffer"`
	StartBeginning bool   `yaml:"startBeginning" json:"startBeginning"`
}

// Configure attempts to configure the Config based on the details entered.
func (c *FileInputConfig) Configure(details map[string]interface{}) error {
	x, ok := details[`path`].(string)
	if !ok {
		return errors.New("missing or invalid path for file input")
	}
	c.Path = x
	c.Buffer = 1000
	if b, ok := details[`buffer`].(int); ok {
		c.Buffer = b
	}
	if b, ok := details[`startBeginning`].(bool); ok {
		c.StartBeginning = b
	}
	return nil
}

// CreateInput creates an Input based on the Config.
func (c *FileInputConfig) CreateInput() (plugin.Input, error) {
	if c.Path == "" {
		return nil, errors.New("no path defined for file input")
	}
	if c.Buffer == 0 {
		c.Buffer = 1000
	}
	return &FileInput{
		Path:           c.Path,
		Buffer:         c.Buffer,
		StartBeginning: c.StartBeginning,
		data:           make(chan []byte, c.Buffer),
		errs:           make(chan error, c.Buffer),
		stopChan:       make(chan struct{}),
		wg:             sync.WaitGroup{},
	}, nil
}

// FileInput works with files as Input.
type FileInput struct {
	Path           string `yaml:"path" json:"path"`
	Buffer         int    `yaml:"buffer" json:"buffer"`
	StartBeginning bool   `yaml:"startBeginning" json:"startBeginning"`
	data           chan []byte
	errs           chan error
	stopChan       chan struct{}
	stopped        bool
	wg             sync.WaitGroup
}

// Start starts the plugin.
func (in *FileInput) Start() error {
	in.wg.Add(1)
	w := 2
	if in.StartBeginning {
		w = 0
	}
	go func() {
		defer in.wg.Done()
		t, err := tail.TailFile(in.Path, tail.Config{Follow: true, Logger: tail.DiscardingLogger, Location: &tail.SeekInfo{Whence: w}})
		for err != nil {
			if in.stopped {
				return
			}
			in.errs <- fmt.Errorf("error adding file %s: %v", in.Path, err)
			time.Sleep(time.Second * 5)
			t, err = tail.TailFile(in.Path, tail.Config{Follow: true})
		}
	fileLoop:
		for {
			select {
			case <-in.stopChan:
				t.Stop()
				break fileLoop
			case line, ok := <-t.Lines:
				if !ok {
					in.errs <- fmt.Errorf("file ended: %v", t.Err())
					break fileLoop
				}
				in.data <- []byte(line.Text)
			}
		}

	}()
	return nil
}

// Stop stops the plugin.
func (in *FileInput) Stop() error {
	in.stopped = true
	close(in.stopChan)
	in.wg.Wait()
	return nil
}

// Source returns the oncoming data channel for the Input Plugin.
func (in *FileInput) Source() <-chan []byte {
	return in.data
}

// Errors returns the error channel for the Input Plugin.
func (in *FileInput) Errors() <-chan error {
	return in.errs
}

// FileOutputConfig contains configuration details when using the FileOutput Plugin.
type FileOutputConfig struct {
	Path   string `yaml:"path" json:"path"`
	Buffer int    `yaml:"buffer" json:"buffer"`
}

// Configure attempts to configure the Config based on the details entered.
func (c *FileOutputConfig) Configure(details map[string]interface{}) error {
	x, ok := details[`path`].(string)
	if !ok {
		return errors.New("missing or invalid path for file output")
	}
	c.Path = x
	c.Buffer = 1000
	b, ok := details[`buffer`].(int)
	if ok {
		c.Buffer = b
	}
	return nil
}

// CreateOutput creates an Input based on the Config.
func (c *FileOutputConfig) CreateOutput() (plugin.Output, error) {
	if c.Path == "" {
		return nil, errors.New("no path defined for file output")
	}
	if c.Buffer == 0 {
		c.Buffer = 1000
	}
	return &FileOutput{
		Path:     c.Path,
		Buffer:   c.Buffer,
		data:     make(chan []byte, c.Buffer),
		errs:     make(chan error, c.Buffer),
		stopChan: make(chan struct{}),
		wg:       sync.WaitGroup{},
	}, nil
}

// FileOutput contains available options for working with files as Input.
type FileOutput struct {
	Path     string `yaml:"path" json:"path"`
	Buffer   int    `yaml:"buffer" json:"buffer"`
	data     chan []byte
	errs     chan error
	stopChan chan struct{}
	stopped  bool
	wg       sync.WaitGroup
}

// Start starts the plugin.
func (out *FileOutput) Start() error {
	out.wg.Add(1)
	go func() {
		defer out.wg.Done()
		syscall.Umask(0)
		f, err := os.OpenFile(out.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		for err != nil {
			if out.stopped {
				return
			}
			out.errs <- fmt.Errorf("error opening file %s for output: %v", out.Path, err)
			time.Sleep(time.Second * 5)
			f, err = os.OpenFile(out.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		}
		defer f.Close()
	fileLoop:
		for {
			select {
			case <-out.stopChan:
				break fileLoop
			case b := <-out.data:
				_, err := f.Write(b)
				if !bytes.HasSuffix(b, []byte{10}) {
					f.Write([]byte{10})
				}
				if err != nil {
					out.errs <- err
				}
			}
		}
	}()
	return nil
}

// Stop stops the plugin.
func (out *FileOutput) Stop() error {
	close(out.stopChan)
	out.wg.Wait()
	return nil
}

// Destination returns the channel used for accept data to the intended Plugin destination.
func (out *FileOutput) Destination() chan<- []byte {
	return out.data
}

// Errors returns the error channel for the Input Plugin.
func (out *FileOutput) Errors() <-chan error {
	return out.errs
}
