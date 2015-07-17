package topic

import (
	"container/list"
	"fmt"
	"io"

	"gopkg.in/logex.v1"

	"github.com/chzyer/mmq/internal/bitmap"
	"github.com/chzyer/mmq/internal/utils"
	"github.com/chzyer/mmq/mmq"
)

const (
	MaxBenchSize = 100
)

var (
	ErrBenchSizeTooLarge = logex.Define("bench size is too large")
)

type Config struct {
	Root      string
	IndexName string
	ChunkBit  uint
}

func (c *Config) Path(name string) string {
	return fmt.Sprintf("%s/%s", c.Root, name)
}

type Instance struct {
	config *Config
	Name   string
	index  int
	file   *bitmap.File
	writer *utils.Writer

	// linked list for Waiters
	waiterList *list.List
	newMsgChan chan struct{}
}

func New(name string, config *Config) (t *Instance, err error) {
	t = &Instance{
		config:     config,
		Name:       name,
		newMsgChan: make(chan struct{}, 1),
	}
	path := config.Path(t.TopicPath())
	t.file, err = bitmap.NewFileEx(path, config.ChunkBit)
	if err != nil {
		return nil, logex.Trace(err)
	}
	// TODO: must read some metafile to determine the offset
	t.writer = &utils.Writer{t.file, 0}
	return t, nil
}

func (t *Instance) TopicPath() string {
	return utils.PathEncode(t.Name)
}

func (t *Instance) Put(m *mmq.Message) error {
	_, err := m.WriteTo(t.writer)
	select {
	case t.newMsgChan <- struct{}{}:
	default:
	}
	return logex.Trace(err)
}

func (t *Instance) get(offset int64, size int, msgChan chan<- []*mmq.Message) ([]*mmq.Message, error) {
	if size > MaxBenchSize {
		return nil, ErrBenchSizeTooLarge.Trace()
	}

	msgs := make([]*mmq.Message, size)
	var (
		msg *mmq.Message
		err error
	)

	var header mmq.HeaderBin

	// check offset
	r := &utils.Reader{t.file, offset}
	p := 0
	for i := 0; i < size; i++ {
		msg, err = mmq.ReadMessage(&header, r, mmq.RF_RESEEK_ON_FAULT)
		if logex.Equal(err, io.ErrUnexpectedEOF) || logex.Equal(err, io.EOF) {
			// use chan, to subscribe
			break
		}
		if err != nil {
			return msgs[:p], logex.Trace(err)
		}
		msgs[p] = msg
		p++
	}

	return msgs[:p], nil
}
