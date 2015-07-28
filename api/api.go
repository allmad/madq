package api

import (
	"bufio"
	"net"

	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/prot"

	"gopkg.in/logex.v1"
)

type Api struct {
	Endpoint string
	conn     net.Conn
	w        *bufio.Writer
}

func New(endpoint string) (*Api, error) {
	conn, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, logex.Trace(err)
	}

	a := &Api{
		Endpoint: endpoint,
		conn:     conn,
		w:        bufio.NewWriter(conn),
	}
	return a, nil
}

func (a *Api) sendMethod(method string, items []prot.Item) error {
	a.w.WriteString(method)
	a.w.WriteByte('\n')
	if err := prot.WriteItems(a.w, items); err != nil {
		return logex.Trace(err)
	}
	return logex.Trace(a.w.Flush())
}

func (a *Api) Get(topicName string, offset int64, size int) error {
	return a.sendMethod("get", []prot.Item{
		prot.NewString(topicName),
		prot.NewInt64(uint64(offset)),
		prot.NewInt64(uint64(size)),
	})
}

func (a *Api) Put(topicName string, msgs []*message.Ins) error {
	return a.sendMethod("put", []prot.Item{
		prot.NewString(topicName),
		prot.NewMsgs(msgs),
	})
}
