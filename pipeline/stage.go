package pipeline

import (
	"context"
	"sync"

	"github.com/jbvmio/lfm/log"
)

// Stage represents a self contained set of functions to process Data.
type Stage struct {
	in         chan Data
	out        chan Data
	errs       chan error
	stopChan   chan struct{}
	wg         sync.WaitGroup
	inputFn    InputFn
	processors []ProcessFn
	outputFn   OutputFn
	CTX        context.Context
	InputFn    DataFunc
	Processors []DataFunc
	OutputFn   DataFunc
	l          log.Logger
}

// NewStage returns a new Stage.
func NewStage(ctx context.Context, l log.Logger) Stage {
	if l == nil {
		l = log.NewNoop()
	}
	return Stage{
		in:       make(chan Data),
		out:      make(chan Data),
		errs:     make(chan error),
		stopChan: make(chan struct{}),
		wg:       sync.WaitGroup{},
		CTX:      ctx,
		l:        l,
	}
}

// In returns the ingesting channel for Data.
func (s *Stage) In() chan Data {
	s.l.Debugf("returning ingest channel")
	return s.in
}

// Out returns the output or end result channel for Data.
func (s *Stage) Out() chan Data {
	s.l.Debugf("returning output channel")
	return s.out
}

// Error returns the error channel.
func (s *Stage) Error() chan error {
	s.l.Debugf("returning error channel")
	return s.errs
}

// DefaultInputFn is used for the InputFn DataFunc.
func (s *Stage) DefaultInputFn(d Data, df DataFunc) <-chan Data {
	s.l.Debugf("receiving input ...")
	out := make(chan Data)
	go func() {
		pass, err := df(d)
		switch {
		case err != nil:
			s.l.Errorf("error receiving input: %v", err)
			s.errs <- err
			close(out)
		case !pass:
			s.l.Debugf("receiving input failed validation, discarding")
			close(out)
		default:
			s.l.Debugf("delivering input data")
			out <- d
			close(out)
		}
	}()
	return out
}

// DefaultProcessorFn is used for all ProcessFn DataFunc.
func (s *Stage) DefaultProcessorFn(in <-chan Data, df DataFunc) <-chan Data {
	s.l.Debugf("begin processing received data")
	out := make(chan Data)
	go func() {
		d, ok := <-in
		if ok {
			pass, err := df(d)
			switch {
			case err != nil:
				s.l.Errorf("error processing data: %v", err)
				s.errs <- err
				close(out)
			case !pass:
				s.l.Debugf("processing data failed validation, discarding ...")
				close(out)
			default:
				out <- d
				close(out)
				s.l.Debugf("processed data successfully")
			}
		}
	}()
	return out
}

// DefaultOutputFn is used for the OutputFn DataFunc.
func (s *Stage) DefaultOutputFn(in <-chan Data, df DataFunc) {
	s.l.Debugf("receiving output ...")
	go func() {
		d, ok := <-in
		if ok {
			pass, err := df(d)
			switch {
			case err != nil:
				s.l.Errorf("error receiving output: %v", err)
				s.errs <- err
			case !pass:
				s.l.Debugf("receiving output failed validation, discarding ...")
			default:
				s.out <- d
				s.l.Debugf("sent output successfully")
			}
		}
	}()
}

// NoopData performs no actions on the given data.
func NoopData(d Data) (bool, error) {
	return true, nil
}

// Run starts processing data through the stage.
func (s *Stage) Run() {
	s.l.Infof("starting ...")
	if s.InputFn == nil {
		s.InputFn = NoopData
	}
	if s.OutputFn == nil {
		s.OutputFn = NoopData
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
	runStage:
		for {
			select {
			case <-s.stopChan:
				s.l.Debugf("received stop signal, stopping ...")
				break runStage
			case <-s.CTX.Done():
				s.l.Debugf("received completion signal, stopping ...")
				break runStage
			case d := <-s.in:
				s.l.Debugf("received data")
				s.wg.Add(1)
				go s.processStage(&s.wg, d)
			}
		}
	}()
}

// Stop stops processing data within the stage.
func (s *Stage) Stop() {
	s.l.Infof("attempting to stop")
	close(s.stopChan)
	s.wg.Wait()
	s.l.Infof("stopped.")
}

func (s *Stage) processStage(wg *sync.WaitGroup, d Data) {
	s.l.Debugf("starting data processing")
	defer wg.Done()
	input := s.DefaultInputFn(d, s.InputFn)
	for n, process := range s.Processors {
		input = s.DefaultProcessorFn(input, process)
		s.l.Debugf("data processing completed processor %d", n)
	}
	s.DefaultOutputFn(input, s.OutputFn)
	s.l.Debugf("completed data processing")
}
