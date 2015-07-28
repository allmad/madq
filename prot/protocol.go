package prot

import (
	"io"

	"gopkg.in/logex.v1"
)

var (
	ErrFlagNotMatch = logex.Define("flag not match")
)

const (
	FlagString byte = 0xa0 + iota
	FlagInt64
	FlagMsgs
	FlagError
	FlagStruct
)

const (
	FlagReply byte = 0xf0
	FlagReq   byte = 0xf1
)

type ItemStruct interface {
	PRead(io.Reader) error
	PWrite(p io.Writer) error
}

type Item interface {
	ItemStruct
	Flag() byte
}

func readItem(r io.Reader, s Item) (err error) {
	if err := check(r, s); err != nil {
		return logex.Trace(err)
	}
	if err = s.PRead(r); err != nil {
		return logex.Trace(err)
	}
	return nil
}

func check(r io.Reader, s Item) error {
	buf := make([]byte, 1)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil
	}
	flag := buf[0]
	if flag != s.Flag() {
		return ErrFlagNotMatch.Trace()
	}
	return nil
}

func writeFlag(w io.Writer, s Item) error {
	_, err := w.Write([]byte{s.Flag()})
	return logex.Trace(err)
}

func WriteReply(w io.Writer, items []Item) error {
	if _, err := w.Write([]byte{FlagReply}); err != nil {
		return logex.Trace(err)
	}
	if err := WriteItems(w, items); err != nil {
		return logex.Trace(err)
	}
	return nil
}

func WriteItems(w io.Writer, items []Item) (err error) {
	for i := 0; i < len(items); i++ {
		if err = writeFlag(w, items[i]); err != nil {
			return logex.Trace(err)
		}
		if err = items[i].PWrite(w); err != nil {
			return logex.Trace(err)
		}
	}
	return
}
