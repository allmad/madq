package qio

import "io"

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

func (r *Reader) Close() (err error) {
	if rc, ok := r.ReaderAt.(io.Closer); ok {
		err = rc.Close()
	}
	return
}
