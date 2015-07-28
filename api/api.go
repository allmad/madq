package api

import (
	"bufio"
	"container/list"
	"io"
	"net"
	"sync"

	"github.com/chzyer/muxque/internal/utils"
	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/prot"
	"github.com/chzyer/muxque/topic"

	"gopkg.in/logex.v1"
)

var (
	MethodGet = prot.NewString("get")
	MethodPut = prot.NewString("put")
)

type Ins struct {
	Endpoint string
	conn     net.Conn
	state    utils.State
	reqQueue *list.List
	reqChan  chan *Request
	w        *bufio.Writer

	sync.Mutex
}

func New(endpoint string) (*Ins, error) {
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, logex.Trace(err)
	}

	a := &Ins{
		Endpoint: endpoint,
		conn:     conn,
		state:    utils.InitState,
		reqQueue: list.New(),
		reqChan:  make(chan *Request, 1<<3),
		w:        bufio.NewWriter(conn),
	}
	go a.readLoop()
	go a.writeLoop()
	return a, nil
}

func (a *Ins) writeLoop() {
	var (
		req *Request
		err error
		w   = bufio.NewWriter(a.conn)
	)

	for {
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
		}
	}
}

func (a *Ins) readLoop() {
	var (
		r          = bufio.NewReader(a.conn)
		packetType byte
		err        error
		req        *Request
		item       *list.Element
	)
	for !a.state.IsClosed() {
		packetType, err = r.ReadByte()
		if err != nil {
			logex.Error(err)
			return
		}
		if packetType == prot.FlagReq {
			logex.Info("req!!")
			return
		}
		if packetType != prot.FlagReply {
			logex.Error("unexpect packetType:", packetType)
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
	w.WriteByte(prot.FlagReq)
	w.Write(r.Method.Bytes())
	w.WriteByte('\n')
	if err := prot.WriteItems(w, r.Args); err != nil {
		return logex.Trace(err)
	}
	return nil
}

func (a *Ins) Get(topicName string, offset int64, size int) error {
	var err prot.Error
	req := NewRequest(MethodGet, []prot.Item{
		prot.NewString(topicName),
		prot.NewInt64(uint64(offset)),
		prot.NewInt64(uint64(size)),
	}, &err)
	a.reqChan <- req
	<-req.Reply
	return err.Err()
}

func (a *Ins) Put(topicName string, msgs []*message.Ins) (int, error) {
	var err topic.PutError
	req := NewRequest(MethodPut, []prot.Item{
		prot.NewString(topicName),
		prot.NewMsgs(msgs),
	}, prot.NewStruct(&err))
	a.reqChan <- req
	<-req.Reply
	return err.N, err.Err
}
