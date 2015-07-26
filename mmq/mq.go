package mmq

import (
	"github.com/chzyer/mmq/message"
	"github.com/chzyer/mmq/topic"
)

type Mmq struct {
	defaultTopicConfig *topic.Config

	topics map[string]*topic.Ins
}

func NewMmq(path string) (*Mmq, error) {
	return nil, nil
}

func (m *Mmq) Put(topicName string, data []*message.Ins) error {
	return nil
}

func (m *Mmq) GetSync(topicName string, offset int64, size int) error {
	// m.topics[topicName].GetSync()
	return nil
}
