package bio

import "encoding/binary"

var _ DiskWriter = new(Writer)

type Writer struct {
	data   []byte
	offset int
}

func NewWriter(data []byte) *Writer {
	return &Writer{data: data}
}

func (w *Writer) Available() int {
	return len(w.data) - w.offset
}

func (w *Writer) WriteDisk(d Diskable) error {
	if w.Available() < d.Size() {
		return ErrWriterBufferFull.Trace()
	}
	d.WriteDisk(w)
	return nil
}

func (w *Writer) Int32(n int32) {
	binary.BigEndian.PutUint32(w.data[w.offset:], uint32(n))
	w.offset += 4
	return
}

func (w *Writer) Skip(n int) {
	w.offset += n
}

func (w *Writer) Byte(b []byte) int {
	n := copy(w.data[w.offset:], b)
	w.offset += n
	return n
}

func (w *Writer) Int64(n int64) {
	binary.BigEndian.PutUint64(w.data[w.offset:], uint64(n))
	w.offset += 8
}
