package api

import (
	"net"
	"sync"
	"testing"

	"gopkg.in/logex.v1"

	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/mq"
	"github.com/chzyer/muxque/topic"
	"github.com/chzyer/muxque/utils"
)

var (
	conf = &topic.Config{
		Root:     utils.GetRoot("/test/api"),
		ChunkBit: 22,
	}
	addr = ":12345"
)

func runClient(m *mq.Muxque, conn net.Conn) {
	mq.NewClient(m, conn)
}

func TestConsumer(t *testing.T) {
	que, ln, err := mq.Listen(addr, conf, runClient)
	if err != nil {
		logex.Fatal(err)
	}
	defer func() {
		ln.Close()
		que.Close()
	}()

	config := &Config{
		Endpoint: ":12345",
		Size:     100,
		Topic:    "test-consumer",
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
	m := message.NewByData(message.NewData([]byte(utils.RandString(256))))
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
