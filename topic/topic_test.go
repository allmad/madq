package topic

import (
	"os"
	"sync"
	"testing"

	"gopkg.in/logex.v1"

	"github.com/chzyer/mmq/internal/utils"
	"github.com/chzyer/mmq/mmq"
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

func BenchmarkTopicPut(b *testing.B) {
	topic, err := New("bench-put", c)
	if err != nil {
		b.Fatal(err)
	}
	msg := mmq.NewMessageByData([]byte(utils.RandString(256)))
	reply := make(chan []error)
	var wg sync.WaitGroup
	go func() {
		for _ = range reply {
			wg.Done()
		}
	}()
	b.ResetTimer()
	buffer := []*mmq.Message{}
	for i := 0; i < b.N; i++ {
		m, _ := mmq.NewMessage(msg.Bytes(), true)
		buffer = append(buffer, m)
		if len(buffer) >= 100 {
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
		incoming := make(chan []*mmq.Message, len(testSource))
		errChan := make(chan error)
		topic.Get(0, len(testSource), incoming, errChan)
		idx := 0
		for {
			select {
			case msg := <-incoming:
				for _, m := range msg {
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
			msg := mmq.NewMessageByData(m)
			errs := topic.PutSync([]*mmq.Message{msg})
			logex.Error(errs)
		}
	}()
	wg.Wait()

}
