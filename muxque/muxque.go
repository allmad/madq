package mq

import (
	"sync"

	"github.com/chzyer/fsmq/muxque/topic"
	"github.com/chzyer/fsmq/rpc"
	"github.com/chzyer/fsmq/rpc/message"

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

func (m *Muxque) deleteTopic(name *rpc.String) {
	m.Lock()
	delete(m.topics, name.String())
	m.Unlock()
}

func (m *Muxque) getTopic(topicName *rpc.String, gen bool) (ins *topic.Ins, err error) {
	name := topicName.String()
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

func (m *Muxque) Delete(topicName *rpc.String, reply chan error) {
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

func (m *Muxque) Put(topicName *rpc.String, data []*message.Ins, reply chan *rpc.PutError) {
	t, err := m.getTopic(topicName, true)
	if err != nil {
		reply <- &rpc.PutError{0, logex.Trace(err)}
		return
	}
	t.Put(data, reply)
	t.Release()
}

func (m *Muxque) Get(topicName *rpc.String, offset int64, size int, reply rpc.ReplyChan, errChan chan error) {
	t, err := m.getTopic(topicName, true)
	if err != nil {
		errChan <- logex.Trace(err)
		return
	}
	t.Get(offset, size, reply, errChan)
	t.Release()
}

func (m *Muxque) Cancel(topicName *rpc.String, offset int64, size int, reply rpc.ReplyChan) error {
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
