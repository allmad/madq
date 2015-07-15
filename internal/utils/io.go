package utils

import "io"

type Reader struct {
	io.ReaderAt
	Offset int64
}

func (p *Reader) Read(val []byte) (n int, err error) {
	n, err = p.ReadAt(val, p.Offset)
	p.Offset += int64(n)
	return
}
