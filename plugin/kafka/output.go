package kafka

import (
	"fmt"
	"os"
	"sync"

	kctl "github.com/jbvmio/kafka"
	"github.com/jbvmio/lfm/plugin"
	"gopkg.in/yaml.v2"
)

const (
	defaultBuffer = 1000
)

// OutputConfig contains configuration details when using the KafkaOutput Plugin.
type OutputConfig struct {
	Brokers []string `yaml:"brokers" json:"brokers"`
	Topics  []string `yaml:"topics" json:"topics"`
}

// Configure attempts to configure the Config based on the details entered.
func (c *OutputConfig) Configure(details map[string]interface{}) error {
	y, err := yaml.Marshal(details)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(y, c)
}

// CreateOutput creates an Input based on the Config.
func (c *OutputConfig) CreateOutput() (plugin.Output, error) {
	hn, err := os.Hostname()
	if err != nil {
		hn = "undiscovered-host"
	}
	conf := kctl.GetConf(hn + `-` + makeHex(6))
	conf.Version = useKafkaVersion
	client, err := kctl.NewCustomClient(conf, c.Brokers...)
	if err != nil {
		return nil, fmt.Errorf("kafka could not create client: %w", err)
	}
	topicsList := filterUnique(c.Topics)
	if ok := topicsExist(client, topicsList...); !ok {
		return nil, fmt.Errorf("kafka could not validate output topics")
	}
	dataChan := make(chan []byte, defaultBuffer)
	errChan := make(chan error, defaultBuffer)
	stopChan := make(chan struct{})
	producers := make([]kafkaProducer, len(topicsList))
	for i := 0; i < len(topicsList); i++ {
		P, err := client.NewProducer()
		if err != nil {
			return nil, fmt.Errorf("kafka could not create producer: %w", err)
		}
		producers[i] = newKafkaProducer(P, topicsList[i], stopChan, errChan)
	}
	return &Output{
		client:    client,
		producers: producers,
		data:      dataChan,
		errs:      errChan,
		stopChan:  stopChan,
		wg:        sync.WaitGroup{},
	}, nil
}

// Output writes data out to a Kafka topic.
type Output struct {
	client    *kctl.KClient
	producers []kafkaProducer
	data      chan []byte
	errs      chan error
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

// Start starts the plugin.
func (out *Output) Start() error {
	for i := 0; i < len(out.producers); i++ {
		out.wg.Add(1)
		out.producers[i].produce(&out.wg)
	}
	out.wg.Add(1)
	go func() {
		defer out.wg.Done()
	produceLoop:
		for {
			select {
			case <-out.stopChan:
				break produceLoop
			case b := <-out.data:
				for i := 0; i < len(out.producers); i++ {
					out.producers[i].send(b)
				}
			}
		}
	}()
	return nil
}

// Stop stops the plugin.
func (out *Output) Stop() error {
	close(out.stopChan)
	out.wg.Wait()
	fmt.Println("all kafka producers stopped.")
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
