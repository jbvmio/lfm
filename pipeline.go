package lfm

import (
	"bytes"
	"context"

	"github.com/jbvmio/lfm/driver"
	"github.com/jbvmio/lfm/log"
	"github.com/jbvmio/lfm/pipeline"
	"github.com/jbvmio/lfm/plugin"
)

// Pipelines is a collection of Pipelines.
type Pipelines struct {
	pls  []Pipeline
	errs chan error
	l    log.Logger
}

// AddPipeline add a Pipeline to the Collection.
func (P *Pipelines) AddPipeline(p Pipeline) {
	P.pls = append(P.pls, p)
}

// UseLogger assigns a logger for the Pipeline collection.
func (P *Pipelines) UseLogger(l log.Logger) {
	P.l = l
}

// Run starts the collection of Pipelines.
func (P *Pipelines) Run() {
	if P.l == nil {
		P.l = log.NewNoop()
	}
	P.l.Info("LFM Starting Pipeline Collection")
	P.errs = make(chan error, len(P.pls)*1000)
	for i := 0; i < len(P.pls); i++ {
		P.l.Info("LFM Starting Pipeline ", P.pls[i].Name)
		P.pls[i].Run(P.errs)
	}
}

// Stop stops the collection of Pipelines.
func (P *Pipelines) Stop() {
	P.l.Info("LFM Stopping Pipeline Collection")
	for i := 0; i < len(P.pls); i++ {
		P.l.Info("LFM Stopping Pipeline ", P.pls[i].Name)
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
	L       log.Logger
}

// Run starts all the Pipeline components.
func (p *Pipeline) Run(errs chan error) {
	if p.L == nil {
		p.L = log.NewNoop()
	}
	p.L.Info("LFM Pipeline Starting")
	p.ctx, p.stop = context.WithCancel(context.Background())
	p.Errs = errs
	p.L.Info("LFM Pipeline Starting", len(p.Inputs), "Inputs")
	for _, x := range p.Inputs {
		x.Start()
	}
	p.L.Info("LFM Pipeline Starting", len(p.Outputs), "Outputs")
	for _, x := range p.Outputs {
		x.Start()
	}
	for _, x := range p.Inputs {
		go p.startIngress(p.ctx, x)
	}
	go p.startEgress(p.ctx, p.Outputs)
	go p.startErrs(p.ctx)
	p.P.Run()
	p.L.Info("LFM Pipeline Started")
}

// Errors returns the error channel for recieving errors.
func (p *Pipeline) Errors() <-chan error {
	p.L.Debug("LFM Pipeline returning error channel")
	return p.Errs
}

// Stop stops all the Pipeline components.
func (p *Pipeline) Stop() {
	p.L.Info("LFM Pipeline Received Stop Request")
	p.stop()
	p.L.Info("LFM Pipeline Stopping ", len(p.Inputs), " Input(s)")
	for _, x := range p.Inputs {
		x.Stop()
	}
	p.L.Info("LFM Pipeline Stopping ", len(p.Outputs), " Output(s)")
	for _, x := range p.Outputs {
		x.Stop()
	}
	p.P.Stop()
	p.L.Info("LFM Pipeline Stopped")
}

func (p *Pipeline) startIngress(ctx context.Context, input plugin.Input) {
	p.L.Info("LFM Pipeline Running Input")
	for data := range input.Source() {
		select {
		case <-ctx.Done():
			p.L.Debug("LFM Pipeline is done, discarding data from Input")
		default:
			p.L.Debug("LFM Pipeline Received Data from Input")
			p.P.In() <- bytes.NewBuffer(data)
		}
	}
	p.L.Info("LFM Pipeline Stopped an Input")
}

func (p *Pipeline) startEgress(ctx context.Context, outputs []plugin.Output) {
	p.L.Info("LFM Pipeline Running ", len(outputs), " Output(s)")
	for data := range p.P.Out() {
		select {
		case <-ctx.Done():
			p.L.Debug("LFM Pipeline is done, skipping send to ", len(outputs), " Output(s)")
		default:
			p.L.Debug("LFM Pipeline Sending Data to ", len(outputs), " Output(s)")
			for _, out := range outputs {
				// add a timeout here or in pipeline/stage lib:
				// timout()
				out.Destination() <- data.Bytes()
			}
		}
	}
	p.L.Info("LFM Pipeline Stopped ", len(outputs), " Output(s)")
}

func (p *Pipeline) startErrs(ctx context.Context) {
	p.L.Info("LFM Pipeline Running Error Monitor")
	for err := range p.P.Error() {
		select {
		case <-ctx.Done():
			p.L.Error("LFM Pipeline is done, received but not sending error: ", err)
		default:
			p.L.Debug("LFM Pipeline Received Error from Error Monitor, Sending", err)
			p.Errs <- err
		}
	}
	p.L.Info("LFM Pipeline Stopped Error Monitor")
}
