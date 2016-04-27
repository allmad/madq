package main

import (
	"github.com/chzyer/flagly"
	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/daemon"
)

type Madq struct {
	Daemon *daemon.Config `flagly:"handler"`
}

func main() {
	madq := new(Madq)
	f := flow.New()

	flagly.Run(madq, f)

	if err := f.Wait(); err != nil {
		logex.Fatal(err)
	}
}
