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
	"go.uber.org/zap"
)

func main() {
	pf := pflag.NewFlagSet(`lfm`, pflag.ExitOnError)
	cfgFile := pf.StringP("config", "c", "./config.yaml", "Path to config Yaml file.")
	pf.Parse(os.Args[1:])

	L := lfm.ConfigureLogger(`info`, os.Stdout)
	defer L.Sync()
	L.Info("Starting LFM ...")

	cfg, err := lfm.ConfigFromFile(*cfgFile)
	if err != nil {
		L.Fatal("error parsing config", zap.Error(err))
	}
	inputs, err := plugins.LoadInputs(cfg)
	if err != nil {
		L.Fatal("error loading inputs", zap.Error(err))
	}
	outputs, err := plugins.LoadOutputs(cfg)
	if err != nil {
		L.Fatal("error loading outputs", zap.Error(err))
	}
	processors, err := drivers.LoadProcessors(cfg)
	if err != nil {
		L.Fatal("error loading processors", zap.Error(err))
	}

	// DEBUG:
	for k := range processors {
		//fmt.Println(k)
		for a, x := range processors[k] {
			//fmt.Println("STAGE:", a)
			for b, y := range x {
				//fmt.Println(" STEP:", b)
				for c := range y {
					//fmt.Println("  DRIVER:", c, ">", z)
					L.Info("discovered drivers", zap.String("PIPELINE", k), zap.Int("STAGE", a), zap.Int("STEP", b), zap.Int("DRIVER", c))
				}
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	var pipelines lfm.Pipelines
	pipelines.UseLogger(L.Sugar())
	for name, input := range inputs {
		output, there := outputs[name]
		if !there {
			panic(`no output for ` + name)
		}
		S := L.With(zap.String(`pipeline`, name)).Sugar()
		stages := processors[name]
		p := pipeline.NewPipeline(ctx, S)
		for n, steps := range stages {
			//sl := l.With(zap.Int(`stage`, n))
			SL := S.With(zap.Int(`stage`, n))
			s := pipeline.NewStage(ctx, SL)
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
			L:       S,
		})
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	L.Info("Starting Pipelines ...")
	pipelines.Run()

	go func(errs <-chan error) {
		for e := range errs {
			fmt.Printf("ERR: %v\n", e)
		}
	}(pipelines.Errors())

	<-sigChan

	L.Info("Stopping Pipelines ...")

	pipelines.Stop()
	cancel()

	L.Info("Finished Syncing Loggers")
	L.Info("Stopped.")
}
