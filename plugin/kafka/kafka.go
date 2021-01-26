package kafka

import (
	"fmt"
	"regexp"
	"sync"

	kctl "github.com/jbvmio/kafka"
)

var useKafkaVersion = kctl.VER210KafkaVersion

type kafkaProducer struct {
	producer *kctl.Producer
	topic    string
	dataChan chan []byte
	stopChan chan struct{}
	errs     chan error
}

func newKafkaProducer(producer *kctl.Producer, topic string, stopChan chan struct{}, errs chan error) kafkaProducer {
	return kafkaProducer{
		producer: producer,
		topic:    topic,
		dataChan: make(chan []byte, defaultBuffer),
		stopChan: stopChan,
		errs:     errs,
	}
}

func (p *kafkaProducer) produce(wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
	produceLoop:
		for {
			select {
			case <-p.stopChan:
				break produceLoop
			case e := <-p.producer.Errors():
				p.errs <- fmt.Errorf("producer for topic %s error: %w", p.topic, e.Err)
			case <-p.producer.Successes():
				// optionally add debug messages here.
			}
		}
		fmt.Printf("producer for topic %s stopped.\n", p.topic)
	}()
}

func (p *kafkaProducer) send(b []byte) {
	go func() {
		// incorporate timeout here if needed.
		p.producer.Input() <- &kctl.Message{
			Topic: p.topic,
			Value: b,
		}
	}()
}

type kafkaProcessor struct {
	dataChan chan []byte
	stopped  *bool
}

func newKafkaProcessor(dataChan chan []byte, stopped *bool) *kafkaProcessor {
	return &kafkaProcessor{
		dataChan: dataChan,
		stopped:  stopped,
	}
}

// ProcessMSG processes a Kafka msg.
func (p *kafkaProcessor) processMSG(msg *kctl.Message) (bool, error) {
	switch {
	case *p.stopped:
		fmt.Println("IS STOPPED")
		return false, nil
	default:
		p.dataChan <- msg.Value
		return true, nil
	}
}

// DeleteCG deletes a consumer group.
func deleteCG(client *kctl.KClient, group string) error {
	var found bool
	groups, errs := client.ListGroups()
	if len(errs) > 1 {
		for _, e := range errs {
			fmt.Println(e)
		}
		return fmt.Errorf("error fetching existing group metadata: %s", errs[0])
	}
	for _, g := range groups {
		if g == group {
			found = true
			break
		}
	}
	if found {
		err := client.RemoveGroup(group)
		if err != nil {
			return fmt.Errorf("error deleting existing group: %w", err)
		}
	}
	return nil
}

// topicsExist returns true if the given topic exists, otherwise false.
func topicsExist(client *kctl.KClient, topics ...string) bool {
	var matched int
	regex := makeRegex(topics...)
	tMeta, err := client.GetTopicMeta()
	if err != nil {
		return false
	}
	dupe := make(map[string]bool)
	for _, t := range tMeta {
		if !dupe[t.Topic] {
			dupe[t.Topic] = true
			if regex.MatchString(t.Topic) {
				matched++
			}
			if matched == len(topics) {
				return true
			}
		}
	}
	return false
}

func makeRegex(terms ...string) *regexp.Regexp {
	var regStr string
	switch len(terms) {
	case 0:
		regStr = ""
	case 1:
		regStr = `^(` + terms[0] + `)$`
	default:
		regStr = `^(` + terms[0]
		for _, t := range terms[1:] {
			regStr += `|` + t
		}
		regStr += `)$`
	}
	return regexp.MustCompile(regStr)
}

// FilterUnique takes an array of strings and returns an array with unique entries.
func filterUnique(vals []string) []string {
	var tmp []string
	dupe := make(map[string]bool)
	for _, v := range vals {
		if !dupe[v] {
			dupe[v] = true
			tmp = append(tmp, v)
		}
	}
	return tmp
}
