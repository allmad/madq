package utils

import (
	"bufio"
	"bytes"
	"io"

	"gopkg.in/logex.v1"
)

var (
	ErrSeekNotSupport = logex.Define("seek with whence(2) is not supported")
)

type Bufio struct {
	*bufio.Reader
	underlay *Reader
}

func NewBufioBlk(b []byte) *Bufio {
	return NewBufio(&Reader{NewBlock(b), 0})
}

func NewBufio(r *Reader) *Bufio {
	b := Bufio{
		Reader:   bufio.NewReader(r),
		underlay: r,
	}
	return &b
}

func (b *Bufio) Offset(o int64) int64 {
	oldOff := b.underlay.Offset - int64(b.Buffered())
	if o >= 0 {
		b.underlay.Seek(o, 0)
		b.Reader.Reset(b.underlay)
	}
	return oldOff
}

type Block struct {
	*bytes.Reader
	Buf []byte
	off int
}

func NewBlock(buf []byte) *Block {
	return &Block{
		Buf:    buf,
		Reader: bytes.NewReader(buf),
	}
}

func (r *Block) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	r.off += n
	return n, err
}

func (r *Block) Bytes() []byte {
	return r.Buf[r.off:]
}

type Reader struct {
	io.ReaderAt
	Offset int64
}

func NewReader(r io.ReaderAt, off int64) *Reader {
	return &Reader{r, off}
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

func (r *Reader) Close() error {
	if rc, ok := r.ReaderAt.(io.Closer); ok {
		return rc.Close()
	}
	return nil
}

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
