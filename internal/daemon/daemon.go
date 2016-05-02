package daemon

import (
	"fmt"

	"github.com/chzyer/flow"
)

type Config struct {
	BaseDir string
}

func (c *Config) FlaglyHandle(f *flow.Flow) {
	println("starting daemon")
	f.Error(fmt.Errorf("something went wrong"))
}
