package mq

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/prot"

	"gopkg.in/logex.v1"
)

var (
	ErrMethodNotFound = logex.Define("method '%v' is not found")
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
	incoming   chan *message.Reply
	wg         sync.WaitGroup
	state      State
	sync.Mutex

	header []byte
}

func NewClient(que *Muxque, conn net.Conn) *Client {
	c := &Client{
		incoming: make(message.Chan),
		que:      que,
		conn:     conn,
		state:    InitState,
	}
	c.initMethod()
	go c.msgLoop()
	go c.readLoop()
	return c
}

func (c *Client) initMethod() {
	c.methods = []*Method{
		NewMethod("get", c.MethodGet),
	}
}

func (c *Client) readLoop() {
	buffer := bufio.NewReader(c.conn)
	c.wg.Add(1)
	defer c.wg.Done()

	var (
		err    error
		method []byte
	)

	for !c.state.IsClosed() {
		method, err = buffer.ReadSlice('\n')
		if err != nil && !logex.Equal(err, ErrMethodNotFound) {
			logex.Error(err)
			break
		}
		c.selectMethod(method, buffer)
	}
	c.close()
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
		return ErrMethodNotFound.Format(string(method))
	}

	return m.Func(buffer)
}

func (c *Client) writeLoop() {
	c.wg.Add(1)
	defer c.wg.Done()
}

func (c *Client) addCtx(ctx *Context) {
	c.Lock()
	c.subscriber[ctx.String()] = ctx
	c.Unlock()
}

func (c *Client) close() bool {
	if !c.state.ToClose() {
		return false
	}
	return true
}

func (c *Client) Close() {
	if c.close() {
		// close all subscribe
	}
}

func (c *Client) msgLoop() {
	for {
		select {
		case ctx, ok := <-c.incoming:
			if !ok {
				break
			}
			_ = ctx
		}
	}
}

func (c *Client) Put(topicName string, msg []*message.Ins) (int, error) {
	n, err := c.que.PutSync(topicName, msg)
	return n, logex.Trace(err)
}

func (c *Client) MethodGet(r *bufio.Reader) error {
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
	return logex.Trace(c.Get(topicName, offset.Int64(), size.Int()))
}

func (c *Client) Get(topicName *prot.String, offset int64, size int) error {
	c.addCtx(&Context{topicName, offset, size})
	return logex.Trace(c.que.GetSync(topicName, offset, size, c.incoming))
}
