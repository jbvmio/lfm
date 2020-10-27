package log

import (
	"io/ioutil"
	L "log"
)

// Logger handles logging.
type Logger interface {
	Debugf(tmpl string, args ...interface{})
	Errorf(tmpl string, args ...interface{})
	Infof(tmpl string, args ...interface{})
	Warnf(tmpl string, args ...interface{})
}

// NewNoop returns a NoopLogger.
func NewNoop() Logger {
	return &noopLogger{
		l: L.New(ioutil.Discard, "[LFM] ", 0),
	}
}

type noopLogger struct {
	l *L.Logger
}

func (n *noopLogger) Debugf(tmpl string, args ...interface{}) {
	n.l.Print(args...)
}

func (n *noopLogger) Errorf(tmpl string, args ...interface{}) {
	n.l.Print(args...)
}

func (n *noopLogger) Infof(tmpl string, args ...interface{}) {
	n.l.Print(args...)
}

func (n *noopLogger) Warnf(tmpl string, args ...interface{}) {
	n.l.Print(args...)
}
