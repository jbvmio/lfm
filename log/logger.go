package log

import (
	"io/ioutil"
	L "log"
)

// Logger handles logging.
type Logger interface {
	Debug(args ...interface{})
	Error(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
}

// NewNoop returns a NoopLogger.
func NewNoop() Logger {
	return &noopLogger{
		l: L.New(ioutil.Discard, "", 0),
	}
}

type noopLogger struct {
	l *L.Logger
}

func (n *noopLogger) Debug(args ...interface{}) {
	n.l.Print(args...)
}

func (n *noopLogger) Error(args ...interface{}) {
	n.l.Print(args...)
}

func (n *noopLogger) Info(args ...interface{}) {
	n.l.Print(args...)
}

func (n *noopLogger) Warn(args ...interface{}) {
	n.l.Print(args...)
}
