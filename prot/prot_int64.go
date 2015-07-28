package prot

import (
	"encoding/binary"
	"io"

	"gopkg.in/logex.v1"
)

type Int64 struct {
	underlay uint64
}

func NewInt64(i uint64) *Int64 {
	return &Int64{i}
}

func ReadInt64(r io.Reader) (o *Int64, err error) {
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

func (i *Int64) PWrite(w io.Writer) error {
	return binary.Write(w, binary.LittleEndian, i.underlay)
}

func (i *Int64) Int64() int64 {
	return int64(i.underlay)
}

func (i *Int64) Int() int {
	return int(i.underlay)
}
