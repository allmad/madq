package topic

import (
	"io"

	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/prot"
	"gopkg.in/logex.v1"
)

type ReplyChan chan<- *Reply
type Chan chan *Reply

type Reply struct {
	Topic string
	Msgs  []*message.Ins
}

func (rp *Reply) PRead(r io.Reader) error {
	pt, err := prot.ReadString(r)
	if err != nil {
		return logex.Trace(err)
	}
	var header message.Header
	pm, err := prot.ReadMsgs(&header, r)
	if err != nil {
		return logex.Trace(err)
	}
	rp.Topic = pt.String()
	rp.Msgs = pm.Msgs()
	return nil
}

func (rp *Reply) PWrite(w io.Writer) error {
	return logex.Trace(prot.WriteItems(w, []prot.Item{
		prot.NewString(rp.Topic),
		prot.NewMsgs(rp.Msgs),
	}))
}

func NewReplyCtx(name string, msgs []*message.Ins) *Reply {
	return &Reply{name, msgs}
}
