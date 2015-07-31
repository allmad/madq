package api

import (
	"bufio"
	"container/list"
	"io"
	"net"
	"sync"

	"github.com/chzyer/muxque/cc"
	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/prot"
	"github.com/chzyer/muxque/rpc"
	"github.com/chzyer/muxque/topic"

	"gopkg.in/logex.v1"
)

var (
	ErrTopicNotFound = logex.Define("topic not found")
)

type Ins struct {
	Endpoint  string
	conn      *net.TCPConn
	state     cc.State
	reqQueue  *list.List
	reqChan   chan *Request
	w         *bufio.Writer
	stopChan  chan struct{}
	replyChan chan *topic.Reply
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
		state:     cc.InitState,
		reqQueue:  list.New(),
		reqChan:   make(chan *Request, 1<<3),
		replyChan: make(chan *topic.Reply, 1024),
		stopChan:  make(chan struct{}),
		w:         bufio.NewWriter(conn),
	}
	go a.readLoop()
	go a.writeLoop()
	return a, nil
}

func (a *Ins) ReplyChan() chan *topic.Reply {
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
	msgStruct := prot.NewStruct(nil)

	for !a.state.IsClosed() {
		pt, err = r.ReadByte()
		if err != nil {
			if !logex.Equal(err, io.EOF) {
				logex.Error(err)
			}
			return
		}
		if pt == prot.FlagMsgPush[0] {
			var reply topic.Reply
			if err = msgStruct.Set(&reply).PSet(r); err != nil {
				logex.Error(err)
				continue
			}
			a.replyChan <- &reply
			continue
		}
		if pt != prot.FlagReply[0] {
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
	var err prot.Error
	a.doReq(rpc.MGet, []prot.Item{
		prot.NewString(topicName),
		prot.NewInt64(uint64(offset)),
		prot.NewInt64(uint64(size)),
	}, &err)
	return err.Err()
}

func (a *Ins) Put(topicName string, msgs []*message.Ins) (int, error) {
	var err topic.PutError
	a.doReq(rpc.MPut, []prot.Item{
		prot.NewString(topicName),
		prot.NewMsgs(msgs),
	}, prot.NewStruct(&err))
	return err.N, err.Err
}

func (a *Ins) doReq(m *prot.String, args []prot.Item, reply prot.Item) {
	req := NewRequest(m, args, reply)
	a.reqChan <- req
	<-req.Reply
}

func (a *Ins) Delete(topicName string) error {
	perr := prot.NewError(nil)
	a.doReq(rpc.MDelete, []prot.Item{
		prot.NewString(topicName),
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
	Method   *prot.String
	Args     []prot.Item
	Reply    chan struct{}
	replyObj prot.Item
	GenReply func(r io.Reader)
}

func NewRequest(method *prot.String, args []prot.Item, reply prot.Item) *Request {
	return &Request{
		Method:   method,
		Args:     args,
		Reply:    make(chan struct{}),
		replyObj: reply,
	}
}

func (r *Request) WriteTo(w *bufio.Writer) error {
	w.Write(prot.FlagReq)
	w.Write(r.Method.Bytes())
	if err := prot.WriteItems(w, r.Args); err != nil {
		return logex.Trace(err)
	}
	return nil
}
