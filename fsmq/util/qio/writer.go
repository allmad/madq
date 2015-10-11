package qio

import (
	"io"

	"gopkg.in/logex.v1"
)

var (
	ErrSeekNotSupport = logex.Define("seek with whence(2) is not supported")
)

type Writer struct {
	io.WriterAt
	Offset int64
}

func NewWriter(w io.WriterAt, off int64) *Writer {
	return &Writer{w, off}
}

func (w *Writer) Write(buf []byte) (n int, err error) {
	n, err = w.WriteAt(buf, w.Offset)
	w.Offset += int64(n)
	return
}

func (w *Writer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		w.Offset = offset
	case 1:
		w.Offset += offset
	case 2:
		return 0, ErrSeekNotSupport.Trace()
	}
	return w.Offset, nil
}

func (w *Writer) Close() error {
	if wc, ok := w.WriterAt.(io.Closer); ok {
		return wc.Close()
	}
	return nil
}
