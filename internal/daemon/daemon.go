package daemon

import "github.com/chzyer/flow"

type Config struct {
	BaseDir string
}

func (c *Config) FlaglyHandle(f *flow.Flow) {
	println(f)
}
