package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jbvmio/lfm"
	"github.com/jbvmio/lfm/internal/drivers"
	"github.com/jbvmio/lfm/internal/plugins"
	"github.com/jbvmio/lfm/pipeline"
	"github.com/spf13/pflag"
)

func main() {
	pf := pflag.NewFlagSet(`lfm`, pflag.ExitOnError)
	cfgFile := pf.StringP("config", "c", "./config.yaml", "Path to config Yaml file.")
	pf.Parse(os.Args[1:])
	cfg, err := lfm.ConfigFromFile(*cfgFile)
	if err != nil {
		fmt.Println("ERR:", err)
		os.Exit(1)
	}
	inputs, err := plugins.LoadInputs(cfg)
	if err != nil {
		fmt.Println("ERR:", err)
		os.Exit(1)
	}
	outputs, err := plugins.LoadOutputs(cfg)
	if err != nil {
		fmt.Println("ERR:", err)
		os.Exit(1)
	}
	processors, err := drivers.LoadProcessors(cfg)
	if err != nil {
		fmt.Println("ERR:", err)
		os.Exit(1)
	}

	// DEBUG:
	for k := range processors {
		fmt.Println(k)
		for a, x := range processors[k] {
			fmt.Println("STAGE:", a)
			for b, y := range x {
				fmt.Println(" STEP:", b)
				for c, z := range y {
					fmt.Println("  DRIVER:", c, ">", z)
				}
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	var pipelines lfm.Pipelines
	for name, input := range inputs {
		output, there := outputs[name]
		if !there {
			panic(`no output for ` + name)
		}
		stages := processors[name]
		p := pipeline.NewPipeline(ctx)
		for _, steps := range stages {
			s := pipeline.NewStage(ctx)
			//s.InputFn = drivers.MakeDriversInitFunc(steps)
			s.Processors = []pipeline.DataFunc{drivers.MakeDriversFunc(steps)}
			//s.OutputFn = drivers.MakeDriversInitFunc(steps)
			p.AddStages(&s)
		}

		pipelines.AddPipeline(lfm.Pipeline{
			Name:    name,
			Inputs:  input,
			Outputs: output,
			P:       p,
		})
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	pipelines.Run()

	go func(errs <-chan error) {
		for e := range errs {
			fmt.Printf("ERR: %v\n", e)
		}
	}(pipelines.Errors())

	<-sigChan

	pipelines.Stop()
	cancel()
}
