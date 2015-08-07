package mq

import (
	"sync"

	"github.com/chzyer/muxque/prot"
	"github.com/chzyer/muxque/prot/message"
	"github.com/chzyer/muxque/topic"
	"gopkg.in/logex.v1"
)

var (
	ErrTopicNotFound = logex.Define("topic not found")
)

type Muxque struct {
	topicCfg *topic.Config
	topics   map[string]*topic.Ins
	sync.RWMutex

	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewMuxque(topicCfg *topic.Config) (*Muxque, error) {
	m := &Muxque{
		topicCfg: topicCfg,
		topics:   make(map[string]*topic.Ins, 1024),
		stopChan: make(chan struct{}),
	}
	return m, nil
}

func (m *Muxque) deleteTopic(name string) {
	m.Lock()
	delete(m.topics, name)
	m.Unlock()
}

func (m *Muxque) getTopic(name string, gen bool) (ins *topic.Ins, err error) {
	m.RLock()
	ins = m.topics[name]
	m.RUnlock()
	if ins != nil {
		if err = ins.Require(); err != nil {
			ins = nil
			err = logex.Trace(err)
		}
		return
	}

	if !gen {
		return nil, ErrTopicNotFound.Trace()
	}

	// make a new one
	m.Lock()
	defer m.Unlock()

	// check again
	ins = m.topics[name]
	if ins != nil {
		if err = ins.Require(); err != nil {
			ins = nil
			err = logex.Trace(err)
		}
		return
	}

	ins, err = topic.New(name, m.topicCfg)
	if err != nil {
		return nil, logex.Trace(err)
	}
	m.topics[name] = ins
	ins.Require()
	return ins, nil
}

func (m *Muxque) Delete(topicName string, reply chan error) {
	t, err := m.getTopic(topicName, true)
	if err != nil {
		reply <- err
		return
	}
	t.Release()
	t.MarkDelete()
	go func() {
		t.SafeDone()
		m.deleteTopic(topicName)
		reply <- nil
	}()
}

func (m *Muxque) Put(topicName string, data []*message.Ins, reply chan *topic.PutError) {
	t, err := m.getTopic(topicName, true)
	if err != nil {
		reply <- &topic.PutError{0, logex.Trace(err)}
		return
	}
	t.Put(data, reply)
	t.Release()
}

func (m *Muxque) Get(topicName *prot.String, offset int64, size int, reply topic.ReplyChan, errChan chan error) {
	t, err := m.getTopic(topicName.String(), true)
	if err != nil {
		errChan <- logex.Trace(err)
		return
	}
	t.Get(offset, size, reply, errChan)
	t.Release()
}

func (m *Muxque) CancelSync(topicName string, offset int64, size int, reply topic.ReplyChan) error {
	t, err := m.getTopic(topicName, false)
	if err != nil {
		return logex.Trace(err)
	}
	err = logex.Trace(t.Cancel(offset, size, reply))
	t.Release()
	return err
}

func (m *Muxque) clientComing() chan struct{} {
	m.wg.Add(1)
	return m.stopChan
}

func (m *Muxque) clientLeaving() {
	m.wg.Done()
}

func (m *Muxque) Close() {
	m.Lock()
	defer m.Unlock()
	for _, t := range m.topics {
		t.MarkClose()
	}
	for _, t := range m.topics {
		t.SafeDone()
	}
	close(m.stopChan)
	m.wg.Wait()
}
