package utils

import (
	"bytes"
	"io"

	"gopkg.in/logex.v1"
)

var (
	ErrSeekNotSupport = logex.Define("seek with whence(2) is not supported")
)

type Buffer struct {
	*bytes.Reader
	Buf []byte
	off int
}

func NewBuffer(buf []byte) *Buffer {
	bb := bytes.NewReader(buf)
	_ = bb
	b := Buffer{
		Buf: buf,
	}
	b.Reader = bb
	return &b
}

func (r *Buffer) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	r.off += n
	return n, err
}

func (r *Buffer) Bytes() []byte {
	return r.Buf[r.off:]
}

type Reader struct {
	io.ReaderAt
	Offset int64
}

func NewReaderBuf(b []byte) *Reader {
	return &Reader{NewBuffer(b), 0}
}

func (r *Reader) Read(val []byte) (n int, err error) {
	n, err = r.ReadAt(val, r.Offset)
	r.Offset += int64(n)
	return
}

func (r *Reader) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case 0:
		r.Offset = offset
	case 1:
		r.Offset += offset
	case 2:
		return 0, ErrSeekNotSupport.Trace()
	}
	return r.Offset, nil
}

type Writer struct {
	io.WriterAt
	Offset int64
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
