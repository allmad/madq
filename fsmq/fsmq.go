package main

import (
	"github.com/chzyer/flagx"

	"gopkg.in/logex.v1"
)

type Config struct {
	Root string
}

func NewConfig() *Config {
	cfg := new(Config)
	flagx.Parse(cfg)
	return cfg
}

func main() {
	cfg := NewConfig()
	logex.Struct(cfg)
}
