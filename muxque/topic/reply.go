package topic

import (
	"io"

	"github.com/chzyer/muxque/rpc"
	"github.com/chzyer/muxque/rpc/message"

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
	pt, err := rpc.ReadString(r)
	if err != nil {
		return logex.Trace(err)
	}
	po, err := rpc.ReadInt64(r)
	if err != nil {
		return logex.Trace(err)
	}
	var header message.Header
	pm, err := rpc.ReadMsgs(&header, r)
	if err != nil {
		return logex.Trace(err)
	}
	rp.Topic = pt.String()
	rp.Offset = po.Int64()
	rp.Msgs = pm.Msgs()
	return nil
}

func (rp *Reply) PWrite(w io.Writer) error {
	return logex.Trace(rpc.WriteItems(w, []rpc.Item{
		rpc.NewString(rp.Topic),
		rpc.NewInt64(uint64(rp.Offset)),
		rpc.NewMsgs(rp.Msgs),
	}))
}
