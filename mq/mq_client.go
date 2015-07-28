package mq

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/prot"
	"github.com/chzyer/muxque/topic"

	"gopkg.in/logex.v1"
)

const (
	MaxMethodSize = 16
)

var (
	ErrMethodNotFound = logex.Define("method '%v' is not found")
	ErrMethodTooLong  = logex.Define("method is too long")
)

type Method struct {
	Name []byte
	Func func(*bufio.Reader) error
}

func NewMethod(name string, f func(*bufio.Reader) error) *Method {
	return &Method{[]byte(name), f}
}

type Context struct {
	Name   *prot.String
	Offset int64
	Size   int
}

func (c *Context) String() string {
	return fmt.Sprintf("%s:%d:%d", c.Name, c.Offset, c.Size)
}

type State uint32

func (s *State) IsClosed() bool {
	return atomic.LoadUint32((*uint32)(s)) == uint32(CloseState)
}

func (s *State) ToClose() bool {
	return atomic.CompareAndSwapUint32((*uint32)(s), uint32(InitState), uint32(CloseState))
}

const (
	InitState State = iota
	CloseState
)

type Client struct {
	methods    []*Method
	que        *Muxque
	conn       net.Conn
	subscriber map[string]*Context
	incoming   chan *topic.Reply
	wg         sync.WaitGroup
	state      State
	stopChan   chan struct{}
	errChan    chan error
	putErrChan chan *topic.PutError
	sync.Mutex
}

func NewClient(que *Muxque, conn net.Conn) *Client {
	c := &Client{
		incoming:   make(topic.Chan),
		que:        que,
		conn:       conn,
		state:      InitState,
		stopChan:   make(chan struct{}),
		errChan:    make(chan error, 1<<3),
		putErrChan: make(chan *topic.PutError, 1<<3),
	}
	c.initMethod()
	go c.readLoop()
	go c.writeLoop()
	return c
}

func (c *Client) initMethod() {
	c.methods = []*Method{
		NewMethod("get", c.Get),
		NewMethod("put", c.Put),
	}
}

func (c *Client) writeLoop() {
	c.wg.Add(1)
	defer func() {
		c.wg.Done()
		c.Close()
	}()

	var (
		err    error
		putErr *topic.PutError
		ctx    *topic.Reply
		w      = bufio.NewWriter(c.conn)
		args   = make([]prot.Item, 1)
	)

	for !c.state.IsClosed() {
		select {
		case err = <-c.errChan:
			if logex.Equal(err, io.EOF) {
				return
			}
			logex.Error(err)
			args[0] = prot.NewError(err)
		case putErr = <-c.putErrChan:
			if logex.Equal(putErr.Err, io.EOF) {
				return
			}
			logex.Error(putErr)
			args[0] = prot.NewStruct(putErr)
		case ctx = <-c.incoming:
			args[0] = prot.NewStruct(ctx)
		case <-c.stopChan:
			return
		}

		err = prot.WriteReply(w, args)
		if err != nil {
			logex.Error(err)
			return
		}
	}
}

func (c *Client) readLoop() {
	buffer := bufio.NewReader(c.conn)
	c.wg.Add(1)
	defer func() {
		c.wg.Done()
		c.Close()
	}()

	var (
		err    error
		method []byte
	)

	for !c.state.IsClosed() {
		method, err = buffer.ReadSlice('\n')
		if err != nil {
			if !logex.Equal(err, io.EOF) {
				logex.Error(err)
			}
			return
		}
		if len(method) > MaxMethodSize {
			logex.Error(ErrMethodTooLong)
			return
		}
		err = c.selectMethod(method[:len(method)-1], buffer)
		if err != nil {
			logex.Error(err)
			return
		}
	}
}

func (c *Client) selectMethod(method []byte, buffer *bufio.Reader) error {
	var m *Method
	for i := 0; i < len(c.methods); i++ {
		if bytes.Equal(method, c.methods[i].Name) {
			m = c.methods[i]
			break
		}
	}
	if m == nil {
		a := strings.TrimSpace(string(method))
		return ErrMethodNotFound.Format(a)
	}

	return m.Func(buffer)
}

func (c *Client) addCtx(ctx *Context) {
	c.Lock()
	c.subscriber[ctx.String()] = ctx
	c.Unlock()
}

func (c *Client) Close() {
	if !c.state.ToClose() {
		return
	}

	close(c.stopChan)
	c.wg.Wait()
	close(c.errChan)
	close(c.incoming)
	c.conn.Close()
}

func (c *Client) Get(r *bufio.Reader) error {
	topicName, err := prot.ReadString(r)
	if err != nil {
		return logex.Trace(err)
	}
	offset, err := prot.ReadInt64(r)
	if err != nil {
		return logex.Trace(err)
	}
	size, err := prot.ReadInt64(r)
	if err != nil {
		return logex.Trace(err)
	}

	c.get(topicName, offset.Int64(), size.Int())
	return nil
}

func (c *Client) get(topicName *prot.String, offset int64, size int) {
	c.addCtx(&Context{topicName, offset, size})
	c.que.Get(topicName, offset, size, c.incoming, c.errChan)
}

func (c *Client) Put(r *bufio.Reader) error {
	topicName, err := prot.ReadString(r)
	if err != nil {
		return logex.Trace(err)
	}

	var header message.Header // move to client field?
	msgs, err := prot.ReadMsgs(&header, r)
	if err != nil {
		return logex.Trace(err)
	}

	c.que.Put(topicName.String(), msgs.Msgs(), c.putErrChan)
	return nil
}
