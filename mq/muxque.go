package mq

import (
	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/topic"
	"gopkg.in/logex.v1"
)

type Muxque struct {
	defTopicCfg *topic.Config
	topics      map[string]*topic.Ins
}

func NewMuxque(topicCfg *topic.Config) (*Muxque, error) {
	m := &Muxque{
		defTopicCfg: topicCfg,
		topics:      make(map[string]*topic.Ins, 1024),
	}
	return m, nil
}

func (m *Muxque) getTopic(name string) (*topic.Ins, error) {
	var err error
	ins := m.topics[name]
	if ins == nil {
		ins, err = topic.New(name, m.defTopicCfg)
		if err != nil {
			return nil, logex.Trace(err)
		}
		m.topics[name] = ins
	}
	return ins, nil
}

func (m *Muxque) PutSync(topicName string, data []*message.Ins) ([]error, error) {
	t, err := m.getTopic(topicName)
	if err != nil {
		return nil, logex.Trace(err)
	}
	return t.PutSync(data), nil
}

func (m *Muxque) GetSync(topicName string, offset int64, size int, reply message.ReplyChan) error {
	t, err := m.getTopic(topicName)
	if err != nil {
		return logex.Trace(err)
	}
	return logex.Trace(t.GetSync(offset, size, reply))
}
