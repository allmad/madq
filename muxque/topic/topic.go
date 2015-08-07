package topic

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chzyer/muxque/rpc"
	"github.com/chzyer/muxque/rpc/message"
	"github.com/chzyer/muxque/utils"
	"github.com/chzyer/muxque/utils/bitmap"
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

const (
	deleteRef = 0x0f000000
	closeRef  = 0x00f00000
	maxReq    = 0x000fffff
	notReqRef = deleteRef | closeRef
)

var (
	ErrMarkDelete = logex.Define("require that which is marked delete")
	ErrMarkClose  = logex.Define("require that which is marked close")
)

type Ins struct {
	Name   string
	config *Config
	index  int
	file   *bitmap.File
	writer *utils.Writer

	// linked list for Waiters
	waiterList *utils.List

	putChan     chan *putArgs
	putChanPool chan *putArgs
	getChan     chan *getArgs
	cancelChan  chan *getArgs
	checkChan   chan struct{}

	ref      int32
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func New(name string, config *Config) (t *Ins, err error) {
	t = &Ins{
		config:      config,
		Name:        name,
		waiterList:  utils.NewList(),
		putChan:     make(chan *putArgs, 1<<3),
		putChanPool: make(chan *putArgs, 1<<3),
		getChan:     make(chan *getArgs, 1<<3),
		cancelChan:  make(chan *getArgs),
		checkChan:   make(chan struct{}, 1),

		stopChan: make(chan struct{}),
	}
	for i := 0; i < cap(t.putChanPool); i++ {
		t.putChanPool <- &putArgs{}
	}
	path := config.Path(t.nameEncoded())
	t.file, err = bitmap.NewFileEx(path, config.ChunkBit)
	if err != nil {
		return nil, logex.Trace(err)
	}

	t.writer = &utils.Writer{t.file, t.file.Size()}
	t.Require()
	go t.ioLoop()
	return t, nil
}

func (t *Ins) nameEncoded() string {
	return utils.PathEncode(t.Name)
}

func (t *Ins) Require() error {
	ref := atomic.AddInt32(&t.ref, 1)

	if ref&notReqRef == 0 {
		return nil
	}
	atomic.AddInt32(&t.ref, -1) // recover
	if ref&deleteRef > 0 {
		return ErrMarkDelete
	}
	return ErrMarkClose
}

func (t *Ins) Release() bool {
	ref := atomic.AddInt32(&t.ref, -1)
	if ref < 0 {
		panic("release to negative")
	}
	if ref&maxReq > 0 || (ref&notReqRef == 0) {
		return false
	}

	close(t.stopChan)
	return true
}

func (t *Ins) MarkClose() {
	ref := atomic.AddInt32(&t.ref, closeRef)
	if ref&deleteRef == 0 {
		t.Release()
	}
}

func (t *Ins) DeleteAndClose() {
	t.MarkDelete()
	t.SafeDone()
}

func (t *Ins) MarkDelete() {
	if atomic.AddInt32(&t.ref, deleteRef)&closeRef == 0 {
		t.Release()
	}
}

func (t *Ins) SafeDone() {
	t.wg.Wait()
	if atomic.LoadInt32(&t.ref)&deleteRef > 0 {
		t.file.Delete()
	}
}

func (t *Ins) ioLoop() {
	t.wg.Add(1)
	defer t.wg.Done()

	var (
		put *putArgs
		get *getArgs

		timer = time.NewTimer(0)
	)
	for {
		select {
		case put = <-t.putChan:
			t.put(put, timer)
			// t.putChanPool <- put
			select {
			case t.checkChan <- struct{}{}:
			default:
			}
		case get = <-t.getChan:
			t.getAsync(get, timer)
		case <-t.checkChan:
			t.checkWaiter()
		case get = <-t.cancelChan:
			t.doCancel(get)
		case <-t.stopChan:
			goto exit
		}

	}

exit:
	close(t.putChan)

	var ok bool
	for {
		select {
		case put, ok = <-t.putChan:
			if !ok {
				return
			}
		default:
			return
		}
	}

}

type putArgs struct {
	msgs  []*message.Ins
	reply chan<- *PutError
}

type PutError struct {
	N   int
	Err error
}

func (p *PutError) PWrite(w io.Writer) (err error) {
	return logex.Trace(rpc.WriteItems(w, []rpc.Item{
		rpc.NewInt64(uint64(p.N)),
		rpc.NewError(p.Err),
	}))
}

func (p *PutError) PRead(r io.Reader) (err error) {
	pn, err := rpc.ReadInt64(r)
	if err != nil {
		return logex.Trace(err)
	}
	perr, err := rpc.ReadError(r)
	if err != nil {
		return logex.Trace(err)
	}
	p.N = pn.Int()
	p.Err = perr.Err()
	return nil
}

func (t *Ins) PutSync(msgs []*message.Ins) (int, error) {
	reply := make(chan *PutError)
	t.Put(msgs, reply)
	ret := <-reply
	return ret.N, ret.Err
}

func (t *Ins) Put(msgs []*message.Ins, reply chan *PutError) {
	// pa := <-t.putChanPool
	pa := new(putArgs)
	pa.msgs = msgs
	pa.reply = reply
	t.putChan <- pa
}

func (t *Ins) put(arg *putArgs, timer *time.Timer) {
	var (
		err error
		i   int
	)
	for ; i < len(arg.msgs); i++ {
		arg.msgs[i].SetMsgId(uint64(t.writer.Offset))
		_, err = arg.msgs[i].WriteTo(t.writer)
		if err != nil {
			break
		}
	}
	arg.reply <- &PutError{i, err}
}

type getArgs struct {
	offset int64
	size   int
	reply  ReplyChan
	err    chan<- error

	// context
	oriOff  int64
	oriSize int
}

func (g *getArgs) String() string {
	return fmt.Sprintf("%v:%v", g.offset, g.size)
}

func (t *Ins) GetSync(offset int64, size int, reply ReplyChan) error {
	errReply := make(chan error)
	t.Get(offset, size, reply, errReply)
	return <-errReply
}

func (t *Ins) Get(offset int64, size int, reply ReplyChan, err chan<- error) {
	t.getChan <- &getArgs{
		offset, size, reply, err,
		offset, size,
	}
}

func (t *Ins) getAsync(arg *getArgs, timer *time.Timer) {
	err := t.get(arg, false)
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
		msg, err = message.Read(&header, r, message.RF_RESEEK_ON_FAULT)
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
		arg.reply <- NewReplyCtx(t.Name, msgs[:p])
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

func (t *Ins) Cancel(offset int64, size int, reply ReplyChan) error {
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
	get.err <- ErrSubscriberNotFound.Format(fmt.Sprintf("%v:%v,%v", get.oriOff, get.oriSize, t.waiterList.Len()))
}

func (t *Ins) checkWaiter() {
	offset := t.writer.Offset
	var err error
	for item := t.waiterList.Front(); item != nil; item = item.Next() {
		waiter := item.Value.(*Waiter)
		if waiter.offset > offset {
			break
		}

		//logex.Info("remove waiter:", waiter.oriOff, waiter.oriSize)
		t.waiterList.Remove(item)
		err = t.get(waiter.toGetArg(nil), false)
		if err != nil {
			logex.Error(err)
		}
	}
}
