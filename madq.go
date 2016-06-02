package main

import (
	"github.com/chzyer/flagly"
	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
)

type Madq struct {
}

func main() {
	madq := new(Madq)
	f := flow.New()

	flagly.Run(madq, f)

	if err := f.Wait(); err != nil {
		logex.Fatal(err)
	}
}
