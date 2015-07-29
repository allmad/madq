package prot

import (
	"io"

	"gopkg.in/logex.v1"
)

type Struct struct {
	underlay ItemStruct
}

func NewStruct(item ItemStruct) *Struct {
	return &Struct{item}
}

func (s *Struct) Set(i ItemStruct) *Struct {
	s.underlay = i
	return s
}

func (s *Struct) Flag() byte {
	return FlagStruct
}

func (s *Struct) PSet(r io.Reader) error {
	return logex.Trace(readItem(r, s))
}

func (s *Struct) PRead(r io.Reader) error {
	return logex.Trace(s.underlay.PRead(r))
}

func (s *Struct) PWrite(w io.Writer) error {
	return logex.Trace(s.underlay.PWrite(w))
}
