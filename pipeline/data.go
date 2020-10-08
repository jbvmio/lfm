package pipeline

// Data represents data traveling through the pipeline.
type Data interface {
	Bytes() []byte
	Read([]byte) (int, error)
	Write([]byte) (int, error)
}

// InputFn ingests input.
type InputFn func(Data, DataFunc) <-chan Data

// ProcessFn processes incoming input and outputs results.
type ProcessFn func(<-chan Data, DataFunc) <-chan Data

// OutputFn outputs results.
type OutputFn func(<-chan Data, DataFunc)

// DataFunc is used by all Input, Process and Output Fns.
type DataFunc func(Data) (bool, error)
