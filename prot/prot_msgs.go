package prot

import (
	"encoding/binary"
	"io"

	"github.com/chzyer/muxque/message"
	"gopkg.in/logex.v1"
)

type Msgs struct {
	buf      *message.Header
	underlay []*message.Ins
}

func NewMsgs(ms []*message.Ins) *Msgs {
	return &Msgs{underlay: ms}
}

func ReadMsgs(buf *message.Header, r io.Reader) (*Msgs, error) {
	s := &Msgs{
		buf: buf,
	}
	if err := readItem(r, s); err != nil {
		return nil, logex.Trace(err)
	}
	return s, nil
}

func (m *Msgs) PSet(r io.Reader) error {
	return logex.Trace(readItem(r, m))
}

func (m *Msgs) Flag() byte {
	return FlagMsgs
}

func (m *Msgs) PRead(r io.Reader) (err error) {
	var length uint16
	if err = binary.Read(r, binary.LittleEndian, &length); err != nil {
		return logex.Trace(err)
	}
	ret := make([]*message.Ins, length)
	i := 0
	for ; i < int(length); i++ {
		ret[i], err = message.Read(m.buf, r, message.RF_DEFAULT)
		if err != nil {
			err = logex.Trace(err)
			break
		}
	}
	m.underlay = ret[:i]
	return
}

func (m *Msgs) PWrite(w io.Writer) (err error) {
	length := uint16(len(m.underlay))
	if err = binary.Write(w, binary.LittleEndian, length); err != nil {
		return logex.Trace(err)
	}
	for i := 0; i < len(m.underlay); i++ {
		if _, err = m.underlay[i].WriteTo(w); err != nil {
			return logex.Trace(err)
		}
	}
	return nil
}

func (m *Msgs) Msgs() []*message.Ins {
	return m.underlay
}
