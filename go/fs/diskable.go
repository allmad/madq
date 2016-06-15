package fs

import (
	"bytes"
	"fmt"
	"io"

	"github.com/chzyer/logex"
)

type Diskable interface {
	Magic() Magic
	DiskItem
}

type DiskItem interface {
	DiskSize() int
	ReadDisk([]byte) error
	WriteDisk([]byte)
}

type DiskWriteItem interface {
	DiskSize() int
	WriteDisk([]byte)
}

type DiskReadItem interface {
	DiskSize() int
	ReadDisk([]byte) error
}

// -----------------------------------------------------------------------------

type DiskBuffer struct {
	rw io.ReadWriter
}

func NewDiskBuffer(rw io.ReadWriter) *DiskBuffer {
	return &DiskBuffer{rw}
}

func (d *DiskBuffer) ReadItem(da Diskable) error {
	buf := make([]byte, da.DiskSize())
	if _, err := io.ReadFull(d.rw, buf); err != nil {
		return logex.Trace(err)
	}
	if err := da.ReadDisk(buf); err != nil {
		return logex.Trace(err)
	}
	return nil
}

func (d *DiskBuffer) WriteItem(da Diskable) error {
	buf := make([]byte, da.DiskSize())

	da.WriteDisk(buf)
	n, err := d.rw.Write(buf)
	if err != nil {
		return logex.Trace(err)
	}
	if n != len(buf) {
		return fmt.Errorf("short written")
	}
	return nil
}

// -----------------------------------------------------------------------------

type DiskReader struct {
	b      []byte
	offset int
}

func NewDiskReader(b []byte) *DiskReader {
	return &DiskReader{b: b}
}

func (r *DiskReader) ReadBytes(n int) []byte {
	buf := r.b[r.offset : r.offset+n]
	r.offset += n
	return buf
}

func (r *DiskReader) ReadItem(d DiskReadItem) error {
	n := d.DiskSize()
	buf := r.b[r.offset : r.offset+n]
	r.offset += n
	return d.ReadDisk(buf)
}

func (r *DiskReader) Skip(n int) {
	r.offset += n
}

func (r *DiskReader) Peek(n int) []byte {
	if n < 0 {
		return r.b[r.offset:]
	}
	return r.b[r.offset : r.offset+n]
}

func (r *DiskReader) ReadItems(ds []DiskReadItem) error {
	var err error
	for _, d := range ds {
		if err = r.ReadItem(d); err != nil {
			return err
		}
	}
	return nil
}

func (r *DiskReader) ReadMagic(d Diskable) error {
	magic := d.Magic()
	magicRead := r.ReadBytes(len(magic))
	if !bytes.Equal(magic, magicRead) {
		return fmt.Errorf("invalid magic: %v, want: %v",
			Magic(magicRead).String(),
			magic.String(),
		)
	}
	return nil
}

// -----------------------------------------------------------------------------

type DiskWriter struct {
	b      []byte
	offset int
}

func NewDiskWriter(b []byte) *DiskWriter {
	return &DiskWriter{b: b}
}

func (w *DiskWriter) WriteMagic(d Diskable) {
	w.WriteBytes(d.Magic())
}

func (w *DiskWriter) WriteBytes(b []byte) {
	n := len(b)
	nw := copy(w.b, b)
	if n != nw {
		panic("not enough memory space")
	}
	w.offset += n
}

func (w *DiskWriter) Skip(n int) {
	w.offset += n
}

func (w *DiskWriter) WriteItem(d DiskWriteItem) {
	n := d.DiskSize()
	d.WriteDisk(w.b[w.offset : w.offset+n])
	w.offset += n
}
