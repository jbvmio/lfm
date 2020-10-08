package kafka

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"

	kctl "github.com/jbvmio/kafka"
	"github.com/jbvmio/lfm/plugin"
	"gopkg.in/yaml.v2"
)

// InputConfig contains configuration details when using the Input Plugin.
type InputConfig struct {
	Brokers     []string `yaml:"brokers" json:"brokers"`
	Topics      []string `yaml:"topics" json:"topics"`
	Group       string   `yaml:"group" json:"group"`
	DeleteGroup bool     `yaml:"deleteGroup" json:"deleteGroup"`
	StartOldest bool     `yaml:"startOldest" json:"startOldest"`
	Threads     int      `yaml:"threads" json:"threads"`
}

// Configure attempts to configure the Config based on the details entered.
func (c *InputConfig) Configure(details map[string]interface{}) error {
	y, err := yaml.Marshal(details)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(y, c)
	if err != nil {
		return err
	}
	if len(c.Brokers) < 1 {
		return fmt.Errorf("missing or invalid brokers defined for kctl input")
	}
	if len(c.Topics) < 1 {
		return fmt.Errorf("missing or invalid topics defined for kctl input")
	}
	if c.Group == "" {
		return fmt.Errorf("missing or invalid group defined for kctl input")
	}
	if c.Threads == 0 {
		c.Threads = 1
	}
	return nil
}

// CreateInput creates an Input based on the Config.
func (c *InputConfig) CreateInput() (plugin.Input, error) {
	hn, err := os.Hostname()
	if err != nil {
		hn = "undiscovered-host"
	}
	conf := kctl.GetConf(hn + `-` + makeHex(6))
	conf.Version = kctl.VER210KafkaVersion
	if c.StartOldest {
		conf.Consumer.Offsets.Initial = -2
	}
	client, err := kctl.NewCustomClient(conf, c.Brokers...)
	if err != nil {
		return nil, fmt.Errorf("kafka could not create client: %w", err)
	}
	if c.DeleteGroup {
		err := deleteCG(client, c.Group)
		if err != nil {
			return nil, fmt.Errorf("kafka could not delete group: %w", err)
		}
	}
	topicsList := filterUnique(c.Topics)
	if ok := topicsExist(client, topicsList...); !ok {
		return nil, fmt.Errorf("kafka could not validate topics")
	}
	stopped := new(bool)
	dataChan := make(chan []byte, 1000)
	processor := newKafkaProcessor(dataChan, stopped)
	consumers := make([]*kctl.ConsumerGroup, c.Threads)
	for i := 0; i < c.Threads; i++ {
		cfg := kctl.GetConf(hn + `-` + makeHex(6))
		consumer, err := kctl.NewConsumerGroup(c.Brokers, c.Group, cfg, topicsList...)
		if err != nil {
			return nil, fmt.Errorf("kafka could not create consumer: %w", err)
		}
		consumer.GETALL(processor.processMSG)
		consumers[i] = consumer
	}
	return &Input{
		client:        client,
		consumers:     consumers,
		group:         c.Group,
		deleteGroup:   c.DeleteGroup,
		data:          dataChan,
		errs:          make(chan error, 1000),
		stopChan:      make(chan struct{}),
		cgStoppedChan: make(chan int, c.Threads),
		stopped:       stopped,
		wg:            sync.WaitGroup{},
	}, nil
}

// Input works with data contained in Kafka Topics as Input.
type Input struct {
	client        *kctl.KClient
	consumers     []*kctl.ConsumerGroup
	group         string
	deleteGroup   bool
	data          chan []byte
	errs          chan error
	stopChan      chan struct{}
	cgStoppedChan chan int
	stopped       *bool
	wg            sync.WaitGroup
}

// Start starts the plugin.
// TODO: Create a "watcher" to restart CG as needed ...
func (in *Input) Start() error {
	for i := 0; i < len(in.consumers); i++ {
		go func(id int, stoppedChan chan int, consumer *kctl.ConsumerGroup) {
			err := consumer.Consume()
			if err != nil {
				in.errs <- err
			}
			stoppedChan <- id
		}(i, in.cgStoppedChan, in.consumers[i])
	}
	return nil
}

// Stop stops the plugin.
func (in *Input) Stop() error {
	*in.stopped = true
	var err error
	var errMsg string
	for i := 0; i < len(in.consumers); i++ {
		errd := in.consumers[i].Close()
		if errd != nil {
			errMsg += errd.Error() + `: `
		}
	}
	if in.deleteGroup {
		if errd := deleteCG(in.client, in.group); errd != nil {
			errMsg += errd.Error() + `: `
		}
	}
	if errd := in.client.Close(); errd != nil {
		errMsg += errd.Error() + `: `
	}
	if errMsg != "" {
		errMsg = strings.TrimSuffix(errMsg, `: `)
		err = fmt.Errorf(errMsg)
	}
	return err
}

// Source returns the oncoming data channel for the Input Plugin.
func (in *Input) Source() <-chan []byte {
	return in.data
}

// Errors returns the error channel for the Input Plugin.
func (in *Input) Errors() <-chan error {
	return in.errs
}

// OutputConfig contains configuration details when using the KafkaOutput Plugin.
type OutputConfig struct {
	Brokers []string `yaml:"brokers" json:"brokers"`
	Topics  []string `yaml:"topic" json:"topic"`
}

// MakeHex returns a random Hex string based on n length.
func makeHex(n int) string {
	b := randomBytes(n)
	hexstring := hex.EncodeToString(b)
	return hexstring
}

func randomBytes(n int) []byte {
	return makeByte(n)
}

func makeByte(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}
