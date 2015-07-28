package prot

import (
	"encoding/binary"
	"io"

	"github.com/chzyer/muxque/message"
	"gopkg.in/logex.v1"
)

var (
	ErrFlagNotMatch = logex.Define("flag not match")
)

const (
	FlagString byte = 0xa0 + iota
	FlagInt64
	FlagMsgs
)

type Reader interface {
	io.Reader
	Peek(n int) ([]byte, error)
	ReadByte() (c byte, err error)
}

type Item interface {
	PRead(io.Reader) error
	Flag() byte
}

func readItem(r Reader, s Item) (err error) {
	if err := check(r, s); err != nil {
		return logex.Trace(err)
	}
	if err = s.PRead(r); err != nil {
		return logex.Trace(err)
	}
	return nil
}

func check(r Reader, s Item) error {
	flag, err := r.ReadByte()
	if err != nil {
		return logex.Trace(err)
	}
	if flag != s.Flag() {
		return ErrFlagNotMatch.Trace()
	}
	return nil
}

type Msgs struct {
	buf      *message.Header
	underlay []*message.Ins
}

func ReadMsgs(buf *message.Header, r Reader) (*Msgs, error) {
	s := &Msgs{
		buf: buf,
	}
	if err := readItem(r, s); err != nil {
		return nil, logex.Trace(err)
	}
	return s, nil
}

func (m *Msgs) Flag() byte {
	return FlagMsgs
}

func (m *Msgs) PRead(r io.Reader) (err error) {
	m.underlay, err = message.ReadSlice(m.buf, r)
	return logex.Trace(err)
}

func (m *Msgs) Msgs() []*message.Ins {
	return m.underlay
}

type String struct {
	underlay []byte
}

func (s *String) String() string {
	return string(s.underlay)
}

func ReadString(r Reader) (*String, error) {
	var s String
	if err := readItem(r, &s); err != nil {
		return nil, logex.Trace(err)
	}
	return &s, nil
}

func (i *String) Flag() byte {
	return FlagString
}

func (i *String) PRead(p io.Reader) error {
	var length uint16
	if err := binary.Read(p, binary.LittleEndian, &length); err != nil {
		return logex.Trace(err)
	}
	i.underlay = make([]byte, int(length))
	if _, err := io.ReadFull(p, i.underlay); err != nil {
		return logex.Trace(err)
	}
	return nil
}

func (i *String) PWrite(w io.Writer) error {
	length := uint16(len(i.underlay))
	if _, err := w.Write([]byte{i.Flag()}); err != nil {
		return logex.Trace(err)
	}
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return logex.Trace(err)
	}
	_, err := w.Write(i.underlay)
	if err != nil {
		return logex.Trace(err)
	}
	return nil
}

type Int64 struct {
	underlay uint64
}

func NewInt64(i uint64) *Int64 {
	return &Int64{i}
}

func ReadInt64(r Reader) (o *Int64, err error) {
	var s Int64
	if err := readItem(r, &s); err != nil {
		return nil, logex.Trace(err)
	}
	return &s, nil
}

func (i *Int64) Flag() byte {
	return FlagInt64
}

func (i *Int64) PRead(p io.Reader) error {
	return binary.Read(p, binary.LittleEndian, &i.underlay)
}

func (i *Int64) PWrite(p io.Writer) error {
	_, err := p.Write([]byte{i.Flag()})
	if err != nil {
		return logex.Trace(err)
	}
	return binary.Write(p, binary.LittleEndian, i.underlay)
}

func (i *Int64) Int64() int64 {
	return int64(i.underlay)
}

func (i *Int64) Int() int {
	return int(i.underlay)
}
