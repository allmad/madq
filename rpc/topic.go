package rpc

import (
	"io"

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

func NewReply(name string, msgs []*message.Ins) *Reply {
	offset := int64(0)
	if len(msgs) > 0 {
		offset = msgs[len(msgs)-1].NextOff()
	}
	return &Reply{name, offset, msgs}
}

func (rp *Reply) PRead(r io.Reader) error {
	pt, err := ReadString(r)
	if err != nil {
		return logex.Trace(err)
	}
	po, err := ReadInt64(r)
	if err != nil {
		return logex.Trace(err)
	}
	var header message.Header
	pm, err := ReadMsgs(&header, r)
	if err != nil {
		return logex.Trace(err)
	}
	rp.Topic = pt.String()
	rp.Offset = po.Int64()
	rp.Msgs = pm.Msgs()
	return nil
}

func (rp *Reply) PWrite(w io.Writer) error {
	return logex.Trace(WriteItems(w, []Item{
		NewString(rp.Topic),
		NewInt64(uint64(rp.Offset)),
		NewMsgs(rp.Msgs),
	}))
}

type PutError struct {
	N   int
	Err error
}

func (p *PutError) PWrite(w io.Writer) (err error) {
	return logex.Trace(WriteItems(w, []Item{
		NewInt64(uint64(p.N)),
		NewError(p.Err),
	}))
}

func (p *PutError) PSize() int {
	return NewInt64(uint64(p.N)).PSize() + NewError(p.Err).PSize()
}

func (p *PutError) PRead(r io.Reader) (err error) {
	pn, err := ReadInt64(r)
	if err != nil {
		return logex.Trace(err)
	}
	perr, err := ReadError(r)
	if err != nil {
		return logex.Trace(err)
	}
	p.N = pn.Int()
	p.Err = perr.Err()
	return nil
}
