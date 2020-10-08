package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/jbvmio/lfm"
	"github.com/jbvmio/lfm/internal/drivers"
	"github.com/jbvmio/lfm/internal/plugins"
	"github.com/jbvmio/lfm/pipeline"
	"github.com/spf13/pflag"
	"github.com/tidwall/gjson"
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

func testInput(d pipeline.Data) (bool, error) {
	if !gjson.ValidBytes(d.Bytes()) {
		return false, fmt.Errorf("invalid json received")
	}
	r := gjson.ParseBytes(d.Bytes())
	ioutil.ReadAll(d)
	//source := r.Get("streamSource")
	//d = bytes.NewBufferString(source.Raw)
	fmt.Println("NA     >>", r.Get(`blah`).Value())
	fmt.Println("Type   >>", r.Type)
	fmt.Println("Type2  >>", r.Type.String())
	fmt.Println("Type3  >>", r.Get("beat").Type.String())
	fmt.Println("Type4  >>", r.Get("beat.name").Type.String())
	fmt.Println("Object >>", r.IsObject())
	fmt.Println("Array  >>", r.IsArray())
	val := r.Value()
	fmt.Printf("Val Type >> %T\n", val)
	fmt.Printf("%+v\n\n", val)
	d.Write([]byte(r.Raw))
	return true, nil
}

func testProcess1(d pipeline.Data) (bool, error) {
	return true, nil
}

func testOutput(d pipeline.Data) (bool, error) {
	return true, nil
}

func makeJSONExtractFunc(path string) func(pipeline.Data) (bool, error) {
	return func(d pipeline.Data) (bool, error) {
		r := gjson.ParseBytes(d.Bytes()).Get(path)
		ioutil.ReadAll(d)
		if !r.Exists() {
			return false, fmt.Errorf("path %s not found", path)
		}
		val := r.Value()
		newOne := make(map[string]interface{})
		newOne[`ugh`] = val
		j, err := json.Marshal(newOne)
		if err != nil {
			return false, err
		}
		d.Write(j)
		return true, nil
	}
}

func makeInputFunc(in <-chan []byte) func(pipeline.Data) (bool, error) {
	return func(d pipeline.Data) (bool, error) {
		data := <-in
		d.Write(data)
		return true, nil
	}
}

func input1(d pipeline.Data) (bool, error) {
	d.Write([]byte(`;input1`))
	return true, nil
}

func stage1processor1(d pipeline.Data) (bool, error) {
	d.Write([]byte(`;Stage1>Processor1`))
	return true, nil
}

func stage1processor2(d pipeline.Data) (bool, error) {
	ugh := d.(testData)
	if ugh.status == `ok 4` {
		d.Write([]byte(`;Stage1>Processor4`))
		return false, nil
	}
	d.Write([]byte(`;Stage1>Processor2`))
	return true, nil
}

func stage2processor1(d pipeline.Data) (bool, error) {
	d.Write([]byte(`;Stage2>Processor1`))
	return true, nil
}

func stage2processor2(d pipeline.Data) (bool, error) {
	ugh := d.(testData)
	if ugh.status == `ok 4` {
		d.Write([]byte(`;Stage2>Processor4`))
	} else {
		d.Write([]byte(`;Stage2>Processor2`))
	}
	return true, nil
}

func output1(d pipeline.Data) (bool, error) {
	d.Write([]byte(`;output1`))
	return true, nil
}

func loadFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

type testData struct {
	status string
	buffer *bytes.Buffer
}

func (t testData) Bytes() []byte {
	return t.buffer.Bytes()
}

func (t testData) Read(b []byte) (int, error) {
	return t.buffer.Read(b)
}

func (t testData) Write(b []byte) (int, error) {
	return t.buffer.Write(b)
}
