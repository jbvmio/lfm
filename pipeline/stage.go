package pipeline

import (
	"context"
	"sync"
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
}

// NewStage returns a new Stage.
func NewStage(ctx context.Context) Stage {
	return Stage{
		in:       make(chan Data),
		out:      make(chan Data),
		errs:     make(chan error),
		stopChan: make(chan struct{}),
		wg:       sync.WaitGroup{},
		CTX:      ctx,
	}
}

// In returns the ingesting channel for Data.
func (s *Stage) In() chan Data {
	return s.in
}

// Out returns the output or end result channel for Data.
func (s *Stage) Out() chan Data {
	return s.out
}

// Error returns the error channel.
func (s *Stage) Error() chan error {
	return s.errs
}

// DefaultInputFn is used for the InputFn DataFunc.
func (s *Stage) DefaultInputFn(d Data, df DataFunc) <-chan Data {
	out := make(chan Data)
	go func() {
		pass, err := df(d)
		switch {
		case err != nil:
			s.errs <- err
			close(out)
		case !pass:
			close(out)
		default:
			out <- d
			close(out)
		}
	}()
	return out
}

// DefaultProcessorFn is used for all ProcessFn DataFunc.
func (s *Stage) DefaultProcessorFn(in <-chan Data, df DataFunc) <-chan Data {
	out := make(chan Data)
	go func() {
		d, ok := <-in
		if ok {
			pass, err := df(d)
			switch {
			case err != nil:
				s.errs <- err
				close(out)
			case !pass:
				close(out)
			default:
				out <- d
				close(out)
			}
		}
	}()
	return out
}

// DefaultOutputFn is used for the OutputFn DataFunc.
func (s *Stage) DefaultOutputFn(in <-chan Data, df DataFunc) {
	go func() {
		d, ok := <-in
		if ok {
			pass, err := df(d)
			switch {
			case err != nil:
				s.errs <- err
			case !pass:
			default:
				s.out <- d
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
				break runStage
			case <-s.CTX.Done():
				break runStage
			case d := <-s.in:
				s.wg.Add(1)
				go s.processStage(&s.wg, d)
				//time.Sleep(time.Millisecond * 1)
			}
		}
	}()
}

// Stop stops processing data within the stage.
func (s *Stage) Stop() {
	close(s.stopChan)
	s.wg.Wait()
}

func (s *Stage) processStage(wg *sync.WaitGroup, d Data) {
	defer wg.Done()
	input := s.DefaultInputFn(d, s.InputFn)
	for _, process := range s.Processors {
		input = s.DefaultProcessorFn(input, process)
	}
	s.DefaultOutputFn(input, s.OutputFn)
}
