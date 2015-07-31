package api

import (
	"net"
	"sync"
	"testing"

	"gopkg.in/logex.v1"

	"github.com/chzyer/muxque/cc"
	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/mq"
	"github.com/chzyer/muxque/topic"
)

var (
	conf = &topic.Config{
		Root:     cc.GetRoot("/test/api"),
		ChunkBit: 22,
	}
	addr = ":12345"
)

func runClient(m *mq.Muxque, conn net.Conn) {
	mq.NewClient(m, conn)
}

type Fataler interface {
	Fatal(...interface{})
}

func runServer(t Fataler) (*mq.Muxque, *net.TCPListener) {
	que, ln, err := mq.Listen(addr, conf, runClient)
	if err != nil {
		t.Fatal(err)
	}
	return que, ln
}

func closeServer(que *mq.Muxque, ln *net.TCPListener) {
	ln.Close()
	que.Close()
}

func TestConsumer(t *testing.T) {
	config := &Config{
		Endpoint: ":12345",
		Size:     100,
		Topic:    "test-consumer",
	}
	if a, err := New(config.Endpoint); err != nil {
		logex.Fatal(err)
	} else if err := a.Delete(config.Topic); err != nil && !logex.Equal(err, ErrTopicNotFound) {
		logex.Fatal(err)
	}

	c, err := NewConsumer(config)
	if err != nil {
		logex.Fatal(err)
	}
	c.Start()

	var wg sync.WaitGroup
	wg.Add(config.Size)

	go func() {
		for reply := range c.ReplyChan {
			println("coming", len(reply.Msgs))
			for _ = range reply.Msgs {
				wg.Done()
			}
		}
	}()

	a, err := New(config.Endpoint)
	if err != nil {
		logex.Fatal(err)
	}
	m := message.NewByData(message.NewData([]byte(cc.RandString(256))))
	msgs := make([]*message.Ins, config.Size)
	for i := 0; i < len(msgs); i++ {
		msgs[i] = m
	}
	n, err := a.Put(config.Topic, msgs)
	if err != nil {
		logex.Fatal(err)
	}
	if n != len(msgs) {
		logex.Fatal("not match")
	}
	wg.Wait()
}
