package loki

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/go-kit/kit/log"
	lclient "github.com/grafana/loki/pkg/promtail/client"
	"github.com/jbvmio/lfm/plugin"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"
)

// OutputConfig contains configuration details when using the StdOutput Plugin.
type OutputConfig struct {
	URL        string        `yaml:"url" json:"url"`
	MaxBackoff time.Duration `yaml:"maxBackoff" json:"maxBackoff"`
	MaxRetries int           `yaml:"maxRetries" json:"maxRetries"`
	MinBackoff time.Duration `yaml:"minBackoff" json:"minBackoff"`
	BatchSize  int           `yaml:"batchSize" json:"batchSize"`
	BatchWait  time.Duration `yaml:"batchWait" json:"batchWait"`
	Timeout    time.Duration `yaml:"timeout" json:"timeout"`
}

// Configure attempts to configure the Config based on the details entered.
func (c *OutputConfig) Configure(details map[string]interface{}) error {
	y, err := yaml.Marshal(details)
	if err != nil {
		return fmt.Errorf("invalid loki output configuration: %w", err)
	}
	err = yaml.Unmarshal(y, c)
	if err != nil {
		return fmt.Errorf("invalid loki output configuration: %w", err)
	}
	_, err = url.Parse(c.URL)
	if err != nil {
		return fmt.Errorf("invalid loki url: %w", err)
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 1 * time.Minute
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.MinBackoff == 0 {
		c.MinBackoff = 5 * time.Second
	}
	if c.BatchSize == 0 {
		c.BatchSize = 100 * 2048
	}
	if c.BatchWait == 0 {
		c.BatchWait = 5 * time.Second
	}
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}
	return nil
}

// CreateOutput creates an Input based on the Config.
func (c *OutputConfig) CreateOutput() (plugin.Output, error) {
	U, err := url.Parse(c.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid loki url: %w", err)
	}
	cfg := lclient.Config{
		URL: flagext.URLValue{URL: U},
		BackoffConfig: util.BackoffConfig{
			MaxBackoff: c.MaxBackoff,
			MaxRetries: c.MaxRetries,
			MinBackoff: c.MinBackoff,
		},
		BatchSize: c.BatchSize,
		BatchWait: c.BatchWait,
		Timeout:   c.Timeout,
	}
	C, err := lclient.New(cfg, log.NewNopLogger())
	if err != nil {
		return nil, fmt.Errorf("could not create loki client: %w", err)
	}
	return &Output{
		loki:     C,
		data:     make(chan []byte),
		errs:     make(chan error),
		stopChan: make(chan struct{}),
		wg:       sync.WaitGroup{},
	}, nil
}

// Output outputs to stdout.
type Output struct {
	loki     lclient.Client
	data     chan []byte
	errs     chan error
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// Entry is the expected object structure to be received by the Loki Output Plugin.
type Entry struct {
	E    string            `json:"entry"`
	TS   time.Time         `json:"timestamp"`
	Tags map[string]string `json:"tags"`
}

// Start starts the plugin.
func (out *Output) Start() error {
	out.wg.Add(1)
	go func() {
		defer out.wg.Done()
	outLoop:
		for {
			select {
			case <-out.stopChan:
				break outLoop
			case input := <-out.data:
				var entry Entry
				err := json.Unmarshal(input, &entry)
				switch {
				case err != nil:
					out.errs <- fmt.Errorf("invalid entry recieved by loki output: %w", err)
				case len(entry.Tags) < 1:
					out.errs <- fmt.Errorf("invalid entry recieved by loki output: no tags defined")
				default:
					if entry.TS == (time.Time{}) {
						entry.TS = time.Now()
					}
					ls := createLabelSet(entry.Tags)
					err := out.loki.Handle(ls, entry.TS, entry.E)
					if err != nil {
						out.errs <- fmt.Errorf("error sending to loki: %w", err)
					}
				}
			}
		}
	}()
	return nil
}

// Stop stops the plugin.
func (out *Output) Stop() error {
	close(out.stopChan)
	out.loki.Stop()
	out.wg.Wait()
	return nil
}

// Destination returns the channel used for accept data to the intended Plugin destination.
func (out *Output) Destination() chan<- []byte {
	return out.data
}

// Errors returns the error channel for the Input Plugin.
func (out *Output) Errors() <-chan error {
	return out.errs
}

func createLabelSet(tags map[string]string) model.LabelSet {
	labelSet := make(model.LabelSet, len(tags))
	for k, v := range tags {
		labelSet[model.LabelName(k)] = model.LabelValue(v)
	}
	return labelSet
}
