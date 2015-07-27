package topic

import (
	"bytes"
	"os"
	"sync"
	"testing"

	"github.com/chzyer/muxque/internal/utils"
	"github.com/chzyer/muxque/message"
	"gopkg.in/logex.v1"
)

var (
	c *Config
)

func init() {
	c = new(Config)
	c.ChunkBit = 22
	c.Root = utils.GetRoot("/test/topic")
	os.MkdirAll(c.Root, 0777)
	os.RemoveAll(c.Root)
}

func BenchmarkTopicGet(b *testing.B) {
	n := b.N
	topic, err := New("bench-get", c)
	if err != nil {
		b.Fatal(err)
	}

	data := message.NewData([]byte(utils.RandString(256)))

	msg := message.NewByData(data)
	var buffer []*message.Ins
	for i := 0; i < n; i++ {
		buffer = append(buffer, msg)
		if len(buffer) > MaxPutBenchSize {
			if _, err := topic.PutSync(buffer); err != nil {
				b.Fatal(err)
			}
			buffer = nil
		}
	}

	b.ResetTimer()
	reply := make(chan *message.Reply, 1024)

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
	msg := message.NewByData(message.NewData([]byte(utils.RandString(256))))
	b.ResetTimer()
	buffer := []*message.Ins{}
	for i := 0; i < b.N; i++ {
		m, _ := message.New(msg.Bytes())
		buffer = append(buffer, m)
		if len(buffer) >= MaxPutBenchSize {
			if _, err := topic.PutSync(buffer); err != nil {
				b.Fatal(err)
			}
			buffer = nil
		}
	}
}

func TestTopicCancel(t *testing.T) {
	topic, err := New("topicCancel", c)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var testSource = [][]byte{
		[]byte("helo"),
		[]byte("who are kkk"),
		[]byte("kjkj"),
	}
	incoming := make(chan *message.Reply, len(testSource))
	incoming2 := make(chan *message.Reply, len(testSource))

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := topic.GetSync(0, len(testSource), incoming); err != nil {
			logex.Error(err)
			t.Error(err)
			return
		}
		msg := <-incoming
		if len(msg.Msgs) == 0 {
			// reget
			msg = <-incoming
		}
		off := msg.Msgs[0].NextOff()
		if err := topic.GetSync(off, len(testSource), incoming2); err != nil {
			logex.Error(err)
			t.Error(err)
			return
		}
		<-incoming2 // empty
	}()
	if _, err := topic.PutSync([]*message.Ins{message.NewByData(
		message.NewData(testSource[0]),
	)}); err != nil {
		t.Fatal(err)
		return
	}
	wg.Wait()
	if err := topic.Cancel(0, len(testSource), incoming); err != nil {
		t.Fatal(err)
	}
	if _, err := topic.PutSync([]*message.Ins{message.NewByData(
		message.NewData(testSource[1]),
	)}); err != nil {
		t.Fatal(err)
		return
	}

	select {
	case msg := <-incoming2:
		if !bytes.Equal(msg.Msgs[0].Data, testSource[1]) {
			t.Fatal("result not expect")
		}
	}

	select {
	case msg := <-incoming:
		logex.Struct(msg)
		t.Fatal("must not come from incoming1")
	default:
	}

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
		incoming := make(chan *message.Reply, len(testSource))
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
			msg := message.NewByData(message.NewData(m))
			if _, err := topic.PutSync([]*message.Ins{msg}); err != nil {
				t.Fatal(err)
			}
		}
	}()
	wg.Wait()

}
