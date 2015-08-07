package api

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/chzyer/muxque/rpc"

	"gopkg.in/logex.v1"
)

type Consumer struct {
	*Config
	Offset    int64
	api       *Ins
	wg        sync.WaitGroup
	stopChan  chan struct{}
	ReplyChan chan *rpc.Reply
	kickChan  chan struct{}
	Remain    int64
}

func NewConsumer(conf *Config) (*Consumer, error) {
	api, err := New(conf.Endpoint)
	if err != nil {
		return nil, logex.Trace(err)
	}
	c := &Consumer{
		api:       api,
		Config:    conf,
		stopChan:  make(chan struct{}),
		ReplyChan: make(chan *rpc.Reply, conf.Size),
		kickChan:  make(chan struct{}, 1),
	}
	return c, nil
}

func (c *Consumer) replyLoop() {
	c.wg.Add(1)
	defer c.wg.Done()
	var (
		reply *rpc.Reply
	)
	for {
		select {
		case reply = <-c.api.replyChan:
		case <-c.api.stopChan:
			return
		}
		if atomic.AddInt64(&c.Remain, -int64(len(reply.Msgs))) == 0 {
			atomic.StoreInt64(&c.Offset, reply.Offset)
			select {
			case c.kickChan <- struct{}{}:
			default:
			}
		}
		c.ReplyChan <- reply
	}
}

func (c *Consumer) reqLoop() {
	c.wg.Add(1)
	defer c.wg.Done()
	for {
		select {
		case <-c.kickChan:
		case <-c.stopChan:
			return
		}
		if err := c.api.Get(c.Topic, atomic.LoadInt64(&c.Offset), c.Size); err != nil {
			logex.Error(err)
			time.Sleep(time.Second)
			select {
			case <-c.kickChan:
			default:
			}
		}
	}
}

func (c *Consumer) Start() {
	select {
	case c.kickChan <- struct{}{}:
	default:
	}
	go c.reqLoop()
	go c.replyLoop()
}

func (c *Consumer) Stop() {
	c.api.Close()
	close(c.api.stopChan)
	c.wg.Wait()
}
