package prot

import (
	"io"

	"gopkg.in/logex.v1"
)

type Error struct {
	underlay []byte
}

func NewError(err error) *Error {
	if err == nil {
		return &Error{nil}
	}
	return &Error{[]byte(err.Error())}
}

func ReadError(r io.Reader) (*Error, error) {
	var e Error
	if err := readItem(r, &e); err != nil {
		return nil, logex.Trace(err)
	}
	return &e, nil
}

func (e *Error) PSet(r io.Reader) error {
	return logex.Trace(readItem(r, e))
}

func (e *Error) PRead(r io.Reader) error {
	s := new(String)
	err := s.PRead(r)
	if err != nil {
		return logex.Trace(err)
	}
	e.underlay = s.underlay
	return nil
}

func (e *Error) Err() error {
	if len(e.underlay) == 0 {
		return nil
	}
	return e
}

func (e *Error) Error() string {
	return string(e.underlay)
}

func (e *Error) PWrite(w io.Writer) error {
	s := &String{e.underlay}
	return logex.Trace(s.PWrite(w))
}

func (e *Error) Flag() byte {
	return FlagError
}
