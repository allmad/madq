package main

import (
	"runtime"

	"github.com/chzyer/flagly"
	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/go/bench"
	"github.com/chzyer/madq/go/debug"
)

type Madq struct {
	CPU   int
	Bench *bench.Config `flagly:"handler"`
	Debug *debug.Config `flagly:"handler"`
}

func (m *Madq) FlaglyEnter() {
	runtime.GOMAXPROCS(m.CPU)
}

func main() {
	madq := new(Madq)
	f := flow.New()

	flagly.Run(madq, f)

	if err := f.Wait(); err != nil {
		logex.Fatal(err)
	}
}
