package topic

import (
	"os"
	"sync"
	"testing"

	"gopkg.in/logex.v1"

	"github.com/chzyer/mmq/internal/utils"
	"github.com/chzyer/mmq/message"
)

var (
	c *Config
)

func init() {
	c = new(Config)
	c.ChunkBit = 22
	c.Root = "/data/mmq/test/topic"
	os.MkdirAll(c.Root, 0777)
	os.RemoveAll(c.Root)
}

func BenchmarkTopicGet(b *testing.B) {
	n := b.N
	topic, err := New("bench-get", c)
	if err != nil {
		b.Fatal(err)
	}

	var wg2 sync.WaitGroup
	replyErrs := make(chan []error)
	go func() {
		for _ = range replyErrs {
			wg2.Done()
		}
	}()
	msg := message.NewMessageByData(message.NewMessageData([]byte(utils.RandString(256))))
	var buffer []*message.Ins
	for i := 0; i < n; i++ {
		buffer = append(buffer, msg)
		if len(buffer) > MaxPutBenchSize {
			wg2.Add(1)
			topic.Put(buffer, replyErrs)
			buffer = nil
		}
	}
	wg2.Wait()
	close(replyErrs)

	b.ResetTimer()
	reply := make(chan *message.ReplyCtx, 1024)

	size := 0
	off := int64(0)
	var wg sync.WaitGroup
	go func() {
		for msgs := range reply {
			wg.Add(1)
			for _, m := range msgs.Msgs {
				off += int64(len(m.Bytes()))
				wg.Done()
			}
			size -= len(msgs.Msgs)
			wg.Done()
			if size == 0 {
				continue
			}
		}
	}()

	for i := 0; i < n; i++ {
		if size < MaxGetBenchSize {
			size++
			continue
		}

		wg.Add(size)
		if err := topic.GetSync(off, size, reply); err != nil {
			b.Fatal(err)
		}
		wg.Wait()
	}
	close(reply)
}

func BenchmarkTopicPut(b *testing.B) {
	topic, err := New("bench-put", c)
	if err != nil {
		b.Fatal(err)
	}
	msg := message.NewMessageByData(message.NewMessageData([]byte(utils.RandString(256))))
	reply := make(chan []error)
	var wg sync.WaitGroup
	go func() {
		for _ = range reply {
			wg.Done()
		}
	}()
	b.ResetTimer()
	buffer := []*message.Ins{}
	for i := 0; i < b.N; i++ {
		m, _ := message.NewMessage(msg.Bytes(), true)
		buffer = append(buffer, m)
		if len(buffer) >= MaxPutBenchSize {
			wg.Add(1)
			topic.Put(buffer, reply)
			buffer = nil
		}
	}
	wg.Wait()
}

func TestTopic(t *testing.T) {
	topic, err := New("topicTest", c)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var testSource = [][]byte{
		[]byte("hello!"),
		[]byte("who are you"),
		[]byte("oo?"),
	}
	wg.Add(len(testSource))
	go func() {
		incoming := make(chan *message.ReplyCtx, len(testSource))
		errChan := make(chan error)
		topic.Get(0, len(testSource), incoming, errChan)
		idx := 0
		for {
			select {
			case msg := <-incoming:
				for _, m := range msg.Msgs {
					if string(m.Data) != string(testSource[idx]) {
						t.Error("result not except", string(m.Data))
					}
					wg.Done()
					idx++
				}
			case err := <-errChan:
				logex.Error("get:", err)
			}
		}
	}()
	go func() {
		for _, m := range testSource {
			msg := message.NewMessageByData(message.NewMessageData(m))
			errs := topic.PutSync([]*message.Ins{msg})
			logex.Error(errs)
		}
	}()
	wg.Wait()

}
