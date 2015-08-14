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

func NewBufio(r *Reader) *Bufio {
	b := Bufio{
		Reader:   bufio.NewReader(r),
		underlay: r,
	}
	return &b
}

func (b *Bufio) UnreadBytes(n int) (err error) {
	offset := b.Offset(-1)
	for i := 0; i < n; i++ {
		if err = b.Reader.UnreadByte(); err != nil {
			b.Offset(offset)
			return err
		}
	}
	return nil
}

func (b *Bufio) Offset(o int64) int64 {
	oldOff := b.underlay.Offset - int64(b.Buffered())
	if o >= 0 {
		b.underlay.Offset = o
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
	bb := bytes.NewReader(buf)
	_ = bb
	b := Block{
		Buf: buf,
	}
	b.Reader = bb
	return &b
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

func NewReaderBlock(b []byte) *Bufio {
	return NewBufio(&Reader{NewBlock(b), 0})
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
