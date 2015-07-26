package mq

import (
	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/topic"
	"gopkg.in/logex.v1"
)

type Mmq struct {
	defaultTopicConfig *topic.Config

	topics map[string]*topic.Ins
}

func NewMmq(path string) (*Mmq, error) {
	return nil, nil
}

func (m *Mmq) getTopic(name string) *topic.Ins {
	var err error
	ins := m.topics[name]
	if ins == nil {
		ins, err = topic.New(name, m.defaultTopicConfig)
		if err != nil {
			panic(err)
		}
		m.topics[name] = ins
	}
	return ins
}

func (m *Mmq) PutSync(topicName string, data []*message.Ins) []error {
	return m.getTopic(topicName).PutSync(data)
}

func (m *Mmq) GetSync(topicName string, offset int64, size int, reply message.ReplyChan) error {
	return logex.Trace(m.getTopic(topicName).GetSync(offset, size, reply))
}
