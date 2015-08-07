package rpc

import (
	"bytes"
	"encoding/binary"
	"io"

	"gopkg.in/logex.v1"
)

type String struct {
	underlay []byte
}

func NewString(s string) *String {
	return &String{[]byte(s)}
}

func (s *String) String() string {
	return string(s.underlay)
}

func (s *String) Equal(b []byte) bool {
	return bytes.Equal(b, s.underlay)
}

func (s *String) Bytes() []byte {
	return s.underlay
}

func ReadString(r io.Reader) (*String, error) {
	var s String
	if err := readItem(r, &s); err != nil {
		return nil, logex.Trace(err)
	}
	return &s, nil
}

func (i *String) PSet(r io.Reader) error {
	return logex.Trace(readItem(r, i))
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
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return logex.Trace(err)
	}
	_, err := w.Write(i.underlay)
	if err != nil {
		return logex.Trace(err)
	}
	return nil
}
