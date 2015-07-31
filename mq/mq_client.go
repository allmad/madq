package mq

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/chzyer/muxque/cc"
	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/prot"
	"github.com/chzyer/muxque/rpc"
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
	Name *prot.String
	Func func(*bufio.Reader) error
}

func NewMethod(name *prot.String, f func(*bufio.Reader) error) *Method {
	return &Method{name, f}
}

type Context struct {
	Name   *prot.String
	Offset int64
	Size   int
}

func (c *Context) String() string {
	return fmt.Sprintf("%s:%d:%d", c.Name, c.Offset, c.Size)
}

type Client struct {
	methods        []*Method
	que            *Muxque
	conn           *net.TCPConn
	subscriber     map[string]*Context
	incoming       chan *topic.Reply
	wg             sync.WaitGroup
	state          cc.State
	stopChan       chan struct{}
	errChan        chan error
	putErrChan     chan *topic.PutError
	parentStopChan chan struct{}
	sync.Mutex
}

func NewClient(que *Muxque, conn *net.TCPConn) *Client {
	c := &Client{
		incoming:       make(topic.Chan),
		que:            que,
		conn:           conn,
		state:          cc.InitState,
		subscriber:     make(map[string]*Context, 1<<3),
		stopChan:       make(chan struct{}),
		errChan:        make(chan error, 1<<3),
		putErrChan:     make(chan *topic.PutError, 1<<3),
		parentStopChan: que.clientComing(),
	}

	c.initMethod()
	go c.readLoop()
	go c.writeLoop()
	return c
}

func (c *Client) initMethod() {
	c.methods = []*Method{
		NewMethod(rpc.MGet, c.Get),
		NewMethod(rpc.MPut, c.Put),
		NewMethod(rpc.MDelete, c.Delete),
		NewMethod(rpc.MPing, c.Ping),
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
		flag   []byte
		w      = bufio.NewWriter(c.conn)
		args   = make([]prot.Item, 1)
	)

	for !c.state.IsClosed() {
		select {
		case err = <-c.errChan:
			if logex.Equal(err, io.EOF) {
				return
			}
			if err != nil {
				logex.Error(err)
			}
			args[0] = prot.NewError(err)
			flag = prot.FlagReply
		case putErr = <-c.putErrChan:
			if logex.Equal(putErr.Err, io.EOF) {
				return
			}
			if putErr.Err != nil {
				logex.Error(putErr)
			}
			args[0] = prot.NewStruct(putErr)
			flag = prot.FlagReply
		case ctx = <-c.incoming:
			args[0] = prot.NewStruct(ctx)
			flag = prot.FlagMsgPush
		case <-c.parentStopChan:
			return
		case <-c.stopChan:
			return
		}

		err = prot.Write(w, flag, args)
		if err == nil {
			err = logex.Trace(w.Flush())
		}
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
		err        error
		method     []byte
		packetType byte
	)

	for !c.state.IsClosed() {
		packetType, err = buffer.ReadByte()
		if err != nil {
			if !logex.Equal(err, io.EOF) {
				logex.Error(err)
			}
			return
		}
		if packetType != prot.FlagReq[0] {
			logex.Error("unexpect packetType:", packetType)
			return
		}
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
		err = c.selectMethod(method, buffer)
		if err != nil {
			logex.Error(err)
			return
		}
	}
}

func (c *Client) selectMethod(method []byte, buffer *bufio.Reader) error {
	var m *Method
	for i := 0; i < len(c.methods); i++ {
		if bytes.Equal(method, c.methods[i].Name.Bytes()) {
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

	logex.Debug("mq_client close")
	close(c.stopChan)
	c.conn.CloseRead()
	c.wg.Wait()
	close(c.errChan)
	close(c.incoming)
	c.conn.Close()
	c.que.clientLeaving()
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

func (c *Client) Delete(r *bufio.Reader) error {
	topicName, err := prot.ReadString(r)
	if err != nil {
		return logex.Trace(err)
	}
	c.que.Delete(topicName.String(), c.errChan)
	return nil
}

func (c *Client) Ping(r *bufio.Reader) error {
	_, err := prot.ReadString(r)
	if err != nil {
		return logex.Trace(err)
	}
	c.errChan <- nil
	return nil
}
