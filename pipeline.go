package lfm

import (
	"bytes"
	"context"

	"github.com/jbvmio/lfm/driver"
	"github.com/jbvmio/lfm/pipeline"
	"github.com/jbvmio/lfm/plugin"
)

// Pipelines is a collection of Pipelines.
type Pipelines struct {
	pls  []Pipeline
	errs chan error
}

// AddPipeline add a Pipeline to the Collection.
func (P *Pipelines) AddPipeline(p Pipeline) {
	P.pls = append(P.pls, p)
}

// Run starts the collection of Pipelines.
func (P *Pipelines) Run() {
	P.errs = make(chan error, len(P.pls)*1000)
	for i := 0; i < len(P.pls); i++ {
		P.pls[i].Errs = P.errs
		P.pls[i].Run()
	}
}

// Stop stops the collection of Pipelines.
func (P *Pipelines) Stop() {
	for i := 0; i < len(P.pls); i++ {
		P.pls[i].Stop()
	}
}

// Errors returns the error channel for recieving errors.
func (P *Pipelines) Errors() <-chan error {
	return P.errs
}

// Pipeline combines all plugins, drivers and stages for processing data.
type Pipeline struct {
	Name    string
	Inputs  []plugin.Input
	Outputs []plugin.Output
	Stages  [][][]driver.Driver
	Errs    chan error
	ctx     context.Context
	stop    context.CancelFunc
	P       pipeline.Pipeline
}

// Run starts all the Pipeline components.
func (p *Pipeline) Run() {
	p.ctx, p.stop = context.WithCancel(context.Background())
	if p.Errs == nil {
		p.Errs = make(chan error, len(p.Inputs)*1000)
	}
	for _, x := range p.Inputs {
		x.Start()
	}
	for _, x := range p.Outputs {
		x.Start()
	}
	for _, x := range p.Inputs {
		go p.startIngress(p.ctx, x)
	}
	go p.startEgress(p.ctx, p.Outputs)
	go p.startErrs(p.ctx)
	p.P.Run()
}

// Errors returns the error channel for recieving errors.
func (p *Pipeline) Errors() <-chan error {
	return p.Errs
}

// Stop stops all the Pipeline components.
func (p *Pipeline) Stop() {
	p.stop()
	for _, x := range p.Inputs {
		x.Stop()
	}
	for _, x := range p.Outputs {
		x.Stop()
	}
	p.P.Stop()
}

func (p *Pipeline) startIngress(ctx context.Context, input plugin.Input) {
ingressLoop:
	for {
		select {
		case <-ctx.Done():
			break ingressLoop
		case data := <-input.Source():
			p.P.In() <- bytes.NewBuffer(data)
			//time.Sleep(time.Millisecond * 100)
		}
	}
}

func (p *Pipeline) startEgress(ctx context.Context, outputs []plugin.Output) {
egressLoop:
	for {
		select {
		case <-ctx.Done():
			break egressLoop
		case data := <-p.P.Out():
			for _, out := range outputs {
				out.Destination() <- data.Bytes()
			}
		}
	}
}

func (p *Pipeline) startErrs(ctx context.Context) {
errLoop:
	for {
		select {
		case <-ctx.Done():
			break errLoop
		case err := <-p.P.Error():
			p.Errs <- err
		}
	}
}
