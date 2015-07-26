package topic

import (
	"fmt"
	"io"
	"time"

	"github.com/chzyer/muxque/internal/bitmap"
	"github.com/chzyer/muxque/internal/utils"
	"github.com/chzyer/muxque/message"
	"gopkg.in/logex.v1"
)

const (
	MaxGetBenchSize = 200
	MaxPutBenchSize = 200
)

var (
	ErrSubscriberNotFound = logex.Define("subscriber for %v is not found")
	ErrBenchSizeTooLarge  = logex.Define("bench size is too large")
	ErrNeedAddToWaiter    = []error{
		io.ErrUnexpectedEOF, io.EOF,
	}
)

type Config struct {
	Root     string
	ChunkBit uint
}

func (c *Config) Path(name string) string {
	return fmt.Sprintf("%s/%s", c.Root, name)
}

type Ins struct {
	Name   string
	config *Config
	index  int
	file   *bitmap.File
	writer *utils.Writer

	// linked list for Waiters
	waiterList *utils.List

	putChan    chan *putArgs
	getChan    chan *getArgs
	checkChan  chan struct{}
	cancelChan chan *getArgs
}

func New(name string, config *Config) (t *Ins, err error) {
	t = &Ins{
		config:     config,
		Name:       name,
		waiterList: utils.NewList(),
		putChan:    make(chan *putArgs, 1<<3),
		getChan:    make(chan *getArgs, 1<<3),
		cancelChan: make(chan *getArgs),
		checkChan:  make(chan struct{}, 1),
	}
	path := config.Path(t.nameEncoded())
	t.file, err = bitmap.NewFileEx(path, config.ChunkBit)
	if err != nil {
		return nil, logex.Trace(err)
	}

	t.writer = &utils.Writer{t.file, t.file.Size()}
	go t.ioLoop()
	return t, nil
}

func (t *Ins) nameEncoded() string {
	return utils.PathEncode(t.Name)
}

func (t *Ins) ioLoop() {
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
				break
			}
			goto put
		case get, ok = <-t.getChan:
			if !ok {
				break
			}
			goto get
		case _, ok = <-t.checkChan:
			if !ok {
				break
			}
			goto check
		case get, ok = <-t.cancelChan:
			if !ok {
				break
			}
			goto cancel
		}

	put:
		t.put(put, timer)
		select {
		case t.checkChan <- struct{}{}:
		default:
		}
		continue

	get:
		t.getAsync(get, timer)
		continue

	check:
		t.checkWaiter()
		continue

	cancel:
		t.doCancel(get)
		continue
	}
}

type putArgs struct {
	msgs  []*message.Ins
	reply chan<- []error
}

func (t *Ins) PutSync(msgs []*message.Ins) []error {
	reply := make(chan []error)
	t.Put(msgs, reply)
	return <-reply
}

func (t *Ins) Put(msgs []*message.Ins, reply chan []error) {
	t.putChan <- &putArgs{msgs, reply}
}

func (t *Ins) put(arg *putArgs, timer *time.Timer) {
	errs := make([]error, len(arg.msgs))
	for i := 0; i < len(arg.msgs); i++ {
		arg.msgs[i].SetMsgId(uint64(t.writer.Offset))
		_, errs[i] = arg.msgs[i].WriteTo(t.writer)
	}
	arg.reply <- errs
}

type getArgs struct {
	offset int64
	size   int
	reply  message.ReplyChan
	err    chan<- error

	// context
	oriOff  int64
	oriSize int
}

func (g *getArgs) String() string {
	return fmt.Sprintf("%v:%v", g.offset, g.size)
}

func (t *Ins) GetSync(offset int64, size int, reply message.ReplyChan) error {
	errReply := make(chan error)
	t.Get(offset, size, reply, errReply)
	return <-errReply
}

func (t *Ins) Get(offset int64, size int, reply message.ReplyChan, err chan<- error) {
	t.getChan <- &getArgs{
		offset, size, reply, err,
		offset, size,
	}
}

func (t *Ins) getAsync(arg *getArgs, timer *time.Timer) {
	err := t.get(arg, true)
	arg.err <- err
}

func (t *Ins) get(arg *getArgs, mustReply bool) error {
	if arg.size > MaxGetBenchSize {
		return ErrBenchSizeTooLarge.Trace(arg.size)
	}

	msgs := make([]*message.Ins, arg.size)
	var (
		msg *message.Ins
		err error
	)

	var header message.Header

	// check offset
	r := &utils.Reader{t.file, arg.offset}
	p := 0
	for i := 0; i < arg.size; i++ {
		msg, err = message.ReadMessage(&header, r, message.RF_RESEEK_ON_FAULT)
		err = logex.Trace(err, i)
		if logex.EqualAny(err, ErrNeedAddToWaiter) {
			// not finish, add to waiterList
			t.addToWaiterList(newWaiter(arg, r.Offset, p))
			break
		}
		if err != nil {
			break
		}
		msgs[p] = msg
		p++
	}

	if mustReply || p > 0 {
		arg.reply <- message.NewReplyCtx(t.Name, msgs[:p])
	}
	if logex.Equal(err, io.EOF) {
		err = nil
	}
	return err
}

func (t *Ins) addToWaiterList(w *Waiter) {
	if t.waiterList.Len() == 0 {
		t.waiterList.PushFront(w)
		return
	}

	for obj := t.waiterList.Front(); obj != nil; obj = obj.Next() {
		if w.offset <= obj.Value.(*Waiter).offset {
			t.waiterList.InsertBefore(w, obj)
			return
		}
		if obj.Next() == nil {
			t.waiterList.PushBack(w)
			return
		}
	}
}

func (t *Ins) Cancel(offset int64, size int, reply message.ReplyChan) error {
	errChan := make(chan error)
	t.cancelChan <- &getArgs{
		err:     errChan,
		oriSize: size,
		reply:   reply,
		oriOff:  offset,
	}
	return <-errChan
}

func (t *Ins) doCancel(get *getArgs) {
	for item := t.waiterList.Front(); item != nil; item = item.Next() {
		waiter := item.Value.(*Waiter)
		if waiter.Equal(get) {
			t.waiterList.Remove(item)
			get.err <- nil
			return
		}
	}
	get.err <- ErrSubscriberNotFound.Format(get.String())
}

func (t *Ins) checkWaiter() {
	offset := t.writer.Offset
	var err error
	for item := t.waiterList.Front(); item != nil; item = item.Next() {
		waiter := item.Value.(*Waiter)
		if waiter.offset > offset {
			break
		}

		t.waiterList.Remove(item)
		err = t.get(waiter.toGetArg(nil), false)
		if err != nil {
			logex.Error(err)
		}
	}
}
