package main

import (
	"runtime"

	"github.com/allmad/madq/go/bench"
	"github.com/allmad/madq/go/debug"
	"github.com/chzyer/flagly"
	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
)

type Madq struct {
	CPU   int           `default:"1"`
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
