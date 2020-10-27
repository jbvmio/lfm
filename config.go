package lfm

import (
	"fmt"
	"io/ioutil"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
)

// Configs contain multiple configurations.
type Configs map[string]Config

// Config details for lfm.
type Config struct {
	Sources      []map[string]interface{} `yaml:"sources"`
	Destinations []map[string]interface{} `yaml:"destinations"`
	Processors   []Stage                  `yaml:"processors"`
}

// Stage holds the stage order and Step definitions.
type Stage struct {
	Stage int    `yaml:"stage"`
	Steps []Step `yaml:"steps"`
}

// Step holds the processing instructions.
type Step struct {
	Step     int                    `yaml:"step"`
	Workflow map[string]interface{} `yaml:"workflow"`
	//Workflow []map[string]interface{} `yaml:"workflow"`
}

// ConfigFromFile loads and returns Configs from a local file.
func ConfigFromFile(path string) (cfgs Configs, err error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return Configs{}, err
	}
	err = yaml.Unmarshal(b, &cfgs)
	return
}

func loadFile(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

// ConfigureLogger return a Logger with the specified loglevel and output.
func ConfigureLogger(logLevel string, ws zapcore.WriteSyncer) *zap.Logger {
	var level zap.AtomicLevel
	var syncOutput zapcore.WriteSyncer
	switch strings.ToLower(logLevel) {
	case "none":
		return zap.NewNop()
	case "", "info":
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "debug":
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "warn":
		level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case "panic":
		level = zap.NewAtomicLevelAt(zap.PanicLevel)
	case "fatal":
		level = zap.NewAtomicLevelAt(zap.FatalLevel)
	default:
		fmt.Printf("Invalid log level supplied. Defaulting to info: %s", logLevel)
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	syncOutput = zapcore.Lock(ws)
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		syncOutput,
		level,
	)
	logger := zap.New(core)
	return logger
}
