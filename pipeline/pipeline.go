package pipeline

import (
	"context"

	"github.com/jbvmio/lfm/log"
)

// Pipeline contains 1 or more Stages.
type Pipeline struct {
	in       chan Data
	out      chan Data
	errs     chan error
	stopChan chan struct{}
	CTX      context.Context
	Stages   []*Stage
	l        log.Logger
}

// NewPipeline returns a new Pipeline.
func NewPipeline(ctx context.Context, l log.Logger) Pipeline {
	if l == nil {
		l = log.NewNoop()
	}
	return Pipeline{
		in:       make(chan Data),
		errs:     make(chan error),
		stopChan: make(chan struct{}),
		CTX:      ctx,
		l:        l,
	}
}

// In returns the ingesting channel for Data.
func (p *Pipeline) In() chan Data {
	p.l.Debugf("returning ingest channel")
	return p.in
}

// Out returns the output or end result channel for Data.
func (p *Pipeline) Out() chan Data {
	p.l.Debugf("returning output channel")
	return p.out
}

// Error returns the error channel.
func (p *Pipeline) Error() chan error {
	p.l.Debugf("returning error channel")
	return p.errs
}

// AddStages adds 1 or more Stages to the Pipeline.
func (p *Pipeline) AddStages(stages ...*Stage) {
	p.l.Infof("adding %d stage(s)", len(stages))
	p.Stages = append(p.Stages, stages...)
}

func (p *Pipeline) configure() {
	linkedChan := p.in
	for i := 0; i < len(p.Stages); i++ {
		p.l.Infof("configuring stage %d", i)
		p.Stages[i].errs = p.errs
		p.Stages[i].in = linkedChan
		linkedChan = p.Stages[i].out
	}
	p.out = linkedChan
}

// Run starts all Stages within the Pipeline.
func (p *Pipeline) Run() {
	p.configure()
	for n, s := range p.Stages {
		p.l.Infof("running stage %d", n)
		s.Run()
	}
}

// Stop stops all Stages within the Pipeline.
func (p *Pipeline) Stop() {
	p.l.Infof("stopping stages")
	for n, s := range p.Stages {
		p.l.Infof("stopping stage %d", n)
		s.Stop()
	}
}
