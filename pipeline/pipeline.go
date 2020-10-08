package pipeline

import "context"

// Pipeline contains 1 or more Stages.
type Pipeline struct {
	in       chan Data
	out      chan Data
	errs     chan error
	stopChan chan struct{}
	CTX      context.Context
	Stages   []*Stage
}

// NewPipeline returns a new Pipeline.
func NewPipeline(ctx context.Context) Pipeline {
	return Pipeline{
		in:       make(chan Data),
		errs:     make(chan error),
		stopChan: make(chan struct{}),
		CTX:      ctx,
	}
}

// In returns the ingesting channel for Data.
func (p *Pipeline) In() chan Data {
	return p.in
}

// Out returns the output or end result channel for Data.
func (p *Pipeline) Out() chan Data {
	return p.out
}

// Error returns the error channel.
func (p *Pipeline) Error() chan error {
	return p.errs
}

// AddStages adds 1 or more Stages to the Pipeline.
func (p *Pipeline) AddStages(stages ...*Stage) {
	p.Stages = append(p.Stages, stages...)
}

func (p *Pipeline) configure() {
	linkedChan := p.in
	for i := 0; i < len(p.Stages); i++ {
		p.Stages[i].errs = p.errs
		p.Stages[i].in = linkedChan
		linkedChan = p.Stages[i].out
	}
	p.out = linkedChan
}

// Run starts all Stages within the Pipeline.
func (p *Pipeline) Run() {
	p.configure()
	for _, s := range p.Stages {
		s.Run()
	}
}

// Stop stops all Stages within the Pipeline.
func (p *Pipeline) Stop() {
	for _, s := range p.Stages {
		s.Stop()
	}
}
