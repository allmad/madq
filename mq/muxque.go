package mq

import (
	"sync"

	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/prot"
	"github.com/chzyer/muxque/topic"
	"gopkg.in/logex.v1"
)

type Muxque struct {
	topicCfg *topic.Config
	topics   map[string]*topic.Ins
	sync.RWMutex
}

func NewMuxque(topicCfg *topic.Config) (*Muxque, error) {
	m := &Muxque{
		topicCfg: topicCfg,
		topics:   make(map[string]*topic.Ins, 1024),
	}
	return m, nil
}

func (m *Muxque) getTopic(name string) (*topic.Ins, error) {
	var err error
	m.RLock()
	ins := m.topics[name]
	m.RUnlock()
	if ins != nil {
		return ins, nil
	}
	m.Lock()
	defer m.Unlock()

	// check again
	ins = m.topics[name]
	if ins != nil {
		return ins, nil
	}

	ins, err = topic.New(name, m.topicCfg)
	if err != nil {
		return nil, logex.Trace(err)
	}
	m.topics[name] = ins
	return ins, nil
}

func (m *Muxque) Put(topicName string, data []*message.Ins, reply chan *topic.PutError) {
	t, err := m.getTopic(topicName)
	if err != nil {
		reply <- &topic.PutError{0, logex.Trace(err)}
		return
	}
	t.Put(data, reply)
}

func (m *Muxque) Get(topicName *prot.String, offset int64, size int, reply message.ReplyChan, errChan chan error) {
	t, err := m.getTopic(topicName.String())
	if err != nil {
		errChan <- logex.Trace(err)
		return
	}
	t.Get(offset, size, reply, errChan)
}

func (m *Muxque) CancelSync(topicName string, offset int64, size int, reply message.ReplyChan) error {
	t, err := m.getTopic(topicName)
	if err != nil {
		return logex.Trace(err)
	}
	return logex.Trace(t.Cancel(offset, size, reply))
}

func (m *Muxque) Close() {
	m.Lock()
	defer m.Unlock()
	for _, t := range m.topics {
		t.Close()
	}
	for _, t := range m.topics {
		t.Wait()
	}
}
