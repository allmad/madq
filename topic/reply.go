package topic

import (
	"io"

	"github.com/chzyer/muxque/prot"
	"github.com/chzyer/muxque/prot/message"
	"gopkg.in/logex.v1"
)

type ReplyChan chan<- *Reply
type Chan chan *Reply

type Reply struct {
	Topic  string
	Offset int64
	Msgs   []*message.Ins
}

func NewReplyCtx(name string, msgs []*message.Ins) *Reply {
	offset := int64(0)
	if len(msgs) > 0 {
		offset = msgs[len(msgs)-1].NextOff()
	}
	return &Reply{name, offset, msgs}
}

func (rp *Reply) PRead(r io.Reader) error {
	pt, err := prot.ReadString(r)
	if err != nil {
		return logex.Trace(err)
	}
	po, err := prot.ReadInt64(r)
	if err != nil {
		return logex.Trace(err)
	}
	var header message.Header
	pm, err := prot.ReadMsgs(&header, r)
	if err != nil {
		return logex.Trace(err)
	}
	rp.Topic = pt.String()
	rp.Offset = po.Int64()
	rp.Msgs = pm.Msgs()
	return nil
}

func (rp *Reply) PWrite(w io.Writer) error {
	return logex.Trace(prot.WriteItems(w, []prot.Item{
		prot.NewString(rp.Topic),
		prot.NewInt64(uint64(rp.Offset)),
		prot.NewMsgs(rp.Msgs),
	}))
}
