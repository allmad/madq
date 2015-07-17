package topic

import (
	"container/list"
	"fmt"
	"io"

	"gopkg.in/logex.v1"

	"time"

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
	putChan    chan *putArgs
	getChan    chan *getArgs
}

func New(name string, config *Config) (t *Instance, err error) {
	t = &Instance{
		config:     config,
		Name:       name,
		newMsgChan: make(chan struct{}, 1),
		putChan:    make(chan *putArgs, 1<<3),
	}
	path := config.Path(t.TopicPath())
	t.file, err = bitmap.NewFileEx(path, config.ChunkBit)
	if err != nil {
		return nil, logex.Trace(err)
	}
	// TODO: must read some metafile to determine the offset
	t.writer = &utils.Writer{t.file, 0}
	go t.ioLoop()
	return t, nil
}

func (t *Instance) TopicPath() string {
	return utils.PathEncode(t.Name)
}

func (t *Instance) notifyNewMsg() {
	select {
	case t.newMsgChan <- struct{}{}:
	default:
	}
}

func (t *Instance) ioLoop() {
	var (
		put *putArgs
		get *getArgs
		ok  bool

		timer = time.NewTimer(0)
	)
	for {
		select {
		case put, ok = <-t.putChan:
			if !ok {
				goto exit
			}
			t.put(put, timer)
		case get, ok = <-t.getChan:
			if !ok {
				goto exit
			}
			t.getAsync(get, timer)
		}
	}
exit:
}

type putArgs struct {
	msgs  []*mmq.Message
	reply chan<- []error
}

func (t *Instance) Put(msgs []*mmq.Message, reply chan []error) {
	t.putChan <- &putArgs{msgs, reply}
}

func (t *Instance) put(arg *putArgs, timer *time.Timer) {
	errs := make([]error, len(arg.msgs))
	for i := 0; i < len(arg.msgs); i++ {
		_, errs[i] = arg.msgs[i].WriteTo(t.writer)
	}
	t.notifyNewMsg()
	timer.Reset(time.Second)
	select {
	case arg.reply <- errs:
	case <-timer.C:
		logex.Error("write reply timeout")
	}
}

type getArgs struct {
	offset int64
	size   int
	reply  chan<- []*mmq.Message
	err    chan<- error
}

func (t *Instance) Get(offset int64, size int, reply chan<- []*mmq.Message, err chan<- error) {
	t.getChan <- &getArgs{offset, size, reply, err}
}

func (t *Instance) getAsync(arg *getArgs, timer *time.Timer) {
	err := t.get(arg)
	timer.Reset(time.Second)
	select {
	case arg.err <- err:
	case <-timer.C:
		logex.Error("reply to get timeout")
	}
}

func (t *Instance) get(arg *getArgs) error {
	if arg.size > MaxBenchSize {
		return ErrBenchSizeTooLarge.Trace()
	}

	msgs := make([]*mmq.Message, arg.size)
	var (
		msg *mmq.Message
		err error
	)

	var header mmq.HeaderBin

	// check offset
	r := &utils.Reader{t.file, arg.offset}
	p := 0
	for i := 0; i < arg.size; i++ {
		msg, err = mmq.ReadMessage(&header, r, mmq.RF_RESEEK_ON_FAULT)
		if logex.Equal(err, io.ErrUnexpectedEOF) || logex.Equal(err, io.EOF) {
			// use chan, to subscribe
			break
		}
		if err != nil {
			break
		}
		msgs[p] = msg
		p++
	}

	select {
	case arg.reply <- msgs[:p]:
	}
	return logex.Trace(err)
}
