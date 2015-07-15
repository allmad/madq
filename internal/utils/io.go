package utils

import "io"

type Reader struct {
	io.ReaderAt
	Offset int64
}

func (r *Reader) Read(val []byte) (n int, err error) {
	n, err = r.ReadAt(val, r.Offset)
	r.Offset += int64(n)
	return
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
