package topic

import (
	"fmt"
	"io"

	"gopkg.in/logex.v1"

	"github.com/chzyer/mmq/internal/bitmap"
	"github.com/chzyer/mmq/internal/utils"
	"github.com/chzyer/mmq/mmq"
)

func Init(c *Config) {
	config = c
}

var (
	config = &Config{
		Root:            "/data/mmq",
		IndexName:       ".index",
		BackendChunkBit: 26,
	}
)

type Config struct {
	Root            string
	IndexName       string
	BackendChunkBit uint
}

func (c *Config) Path(name string) string {
	return fmt.Sprintf("%s/%s", c.Root, name)
}

type Instance struct {
	Name string
	index
	file   *bitmap.File
	writer io.Writer
}

func New(name string) (t *Instance, err error) {
	t = &Instance{
		Name: name,
	}
	t.file, err = bitmap.NewFile(config.Path(t.TopicPath()), config.BackendChunkBit)
	if err != nil {
		return nil, logex.Trace(err)
	}
	t.writer = utils.Reader{t.file, 0}
	return t, nil
}

func (t *Instance) TopicPath() string {
	return utils.PathEncode(t.Name)
}

func (t *Instance) Put(m *mmq.Message) error {
	return logex.Trace(m.WriteTo(t.writer))
}
