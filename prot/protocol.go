package prot

import (
	"encoding/binary"
	"io"

	"gopkg.in/logex.v1"
)

var (
	ErrFlagNotMatch = logex.Define("flag not match")
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

type String struct {
	underlay []byte
}

func (s *String) String() string {
	return string(s.underlay)
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

func ReadString(r Reader) (*String, error) {
	var s String
	if err := readItem(r, &s); err != nil {
		return nil, logex.Trace(err)
	}
	return &s, nil
}

func (i *String) Flag() byte {
	return 0xa0
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

type Int64 struct {
	underlay uint64
}

func ReadInt64(r Reader) (o *Int64, err error) {
	var s Int64
	if err := readItem(r, &s); err != nil {
		return nil, logex.Trace(err)
	}
	return &s, nil
}

func (i *Int64) Flag() byte {
	return 0xa1
}

func (i *Int64) PRead(p io.Reader) error {
	return binary.Read(p, binary.LittleEndian, &i.underlay)
}

func (i *Int64) Int64() int64 {
	return int64(i.underlay)
}

func (i *Int64) Int() int {
	return int(i.underlay)
}
