package plugin

// TypeID are used to assign IDs to available Plugins.
type TypeID int

// Available PluginTypes:
const (
	TypeNone TypeID = iota
	TypeInputFile
	TypeInputKafka
	TypeOutputFile
	TypeOutputStd
	TypeOutputLoki
)

var idStrings = [...]string{
	`none`,
	`FileInput`,
	`KafkaInput`,
	`FileOutput`,
	`StdOutput`,
	`LokiOutput`,
}

func (id TypeID) String() string {
	return idStrings[id]
}

// Plugin represents input or output plugins.
type Plugin interface {
	// Start should initialize and start the Plugin, assigning defaults and return any errors if misconfigured.
	Start() error
	// Stop should stop the Plugin, returning any errors.
	Stop() error
	// Errors channel is used to send or receive errors while the Plugin is running.
	Errors() <-chan error
}

// Input works with sources of data.
type Input interface {
	Plugin
	Source() <-chan []byte
}

// Output works with storing of data.
type Output interface {
	Plugin
	Destination() chan<- []byte
}
