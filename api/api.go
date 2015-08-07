package api

import (
	"bufio"
	"container/list"
	"io"
	"net"
	"sync"

	"github.com/chzyer/muxque/rpc"
	"github.com/chzyer/muxque/rpc/message"
	"github.com/chzyer/muxque/utils"

	"gopkg.in/logex.v1"
)

var (
	ErrTopicNotFound = logex.Define("topic not found")
)

type Ins struct {
	Endpoint  string
	conn      *net.TCPConn
	state     utils.State
	reqQueue  *list.List
	reqChan   chan *Request
	w         *bufio.Writer
	stopChan  chan struct{}
	replyChan chan *rpc.Reply
	wg        sync.WaitGroup

	sync.Mutex
}

func New(endpoint string) (*Ins, error) {
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, logex.Trace(err)
	}

	a := &Ins{
		Endpoint:  endpoint,
		conn:      conn.(*net.TCPConn),
		state:     utils.InitState,
		reqQueue:  list.New(),
		reqChan:   make(chan *Request, 1<<3),
		replyChan: make(chan *rpc.Reply, 1024),
		stopChan:  make(chan struct{}),
		w:         bufio.NewWriter(conn),
	}
	go a.readLoop()
	go a.writeLoop()
	return a, nil
}

func (a *Ins) ReplyChan() chan *rpc.Reply {
	return a.replyChan
}

func (a *Ins) writeLoop() {
	a.wg.Add(1)
	defer func() {
		a.wg.Done()
		a.Close()
	}()
	var (
		req *Request
		err error
		w   = bufio.NewWriter(a.conn)
	)

	for !a.state.IsClosed() {
		select {
		case req = <-a.reqChan:
			if err = req.WriteTo(w); err != nil {
				return
			}
			if err = w.Flush(); err != nil {
				return
			}
			a.Lock()
			a.reqQueue.PushBack(req)
			a.Unlock()
		case <-a.stopChan:
			return
		}
	}
}

func (a *Ins) readLoop() {
	a.wg.Add(1)
	defer func() {
		a.wg.Done()
		a.Close()
	}()

	var (
		pt   byte
		err  error
		req  *Request
		item *list.Element
	)

	r := bufio.NewReader(a.conn)
	msgStruct := rpc.NewStruct(nil)

	for !a.state.IsClosed() {
		pt, err = r.ReadByte()
		if err != nil {
			if !logex.Equal(err, io.EOF) {
				logex.Error(err)
			}
			return
		}
		if pt == rpc.FlagMsgPush[0] {
			var reply rpc.Reply
			if err = msgStruct.Set(&reply).PSet(r); err != nil {
				logex.Error(err)
				continue
			}
			a.replyChan <- &reply
			continue
		}
		if pt != rpc.FlagReply[0] {
			logex.Error("unexpect packetType:", pt)
			return
		}
		a.Lock()
		item = a.reqQueue.Front()
		a.reqQueue.Remove(item)
		req = item.Value.(*Request)
		a.Unlock()
		if err = req.replyObj.PSet(r); err != nil {
			logex.Error(err)
			return
		}
		req.Reply <- struct{}{}
	}
	return
}

func (a *Ins) Close() {
	if !a.state.ToClose() {
		return
	}
	close(a.stopChan)
	a.conn.CloseRead()
	a.wg.Wait()
	a.conn.CloseWrite()
}

func (a *Ins) Get(topicName string, offset int64, size int) error {
	var err rpc.Error
	a.doReq(rpc.MGet, []rpc.Item{
		rpc.NewString(topicName),
		rpc.NewInt64(uint64(offset)),
		rpc.NewInt64(uint64(size)),
	}, &err)
	return err.Err()
}

func (a *Ins) Put(topicName string, msgs []*message.Ins) (int, error) {
	var err rpc.PutError
	a.doReq(rpc.MPut, []rpc.Item{
		rpc.NewString(topicName),
		rpc.NewMsgs(msgs),
	}, rpc.NewStruct(&err))
	return err.N, err.Err
}

func (a *Ins) doReq(m *rpc.String, args []rpc.Item, reply rpc.Item) {
	req := NewRequest(m, args, reply)
	a.reqChan <- req
	<-req.Reply
}

func (a *Ins) Ping(payload *rpc.String) error {
	perr := rpc.NewError(nil)
	a.doReq(rpc.MPing, []rpc.Item{payload}, perr)
	return perr.Err()
}

func (a *Ins) Delete(topicName string) error {
	perr := rpc.NewError(nil)
	a.doReq(rpc.MDelete, []rpc.Item{
		rpc.NewString(topicName),
	}, perr)
	err := perr.Err()
	if err == nil {
		return nil
	}
	if err.Error() == ErrTopicNotFound.Error() {
		err = ErrTopicNotFound.Trace()
	}
	return err
}

type Request struct {
	Method   *rpc.String
	Args     []rpc.Item
	Reply    chan struct{}
	replyObj rpc.Item
	GenReply func(r io.Reader)
}

func NewRequest(method *rpc.String, args []rpc.Item, reply rpc.Item) *Request {
	return &Request{
		Method:   method,
		Args:     args,
		Reply:    make(chan struct{}),
		replyObj: reply,
	}
}

func (r *Request) WriteTo(w *bufio.Writer) error {
	w.Write(rpc.FlagReq)
	w.Write(r.Method.Bytes())
	if err := rpc.WriteItems(w, r.Args); err != nil {
		return logex.Trace(err)
	}
	return nil
}
