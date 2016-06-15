package disk

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/chzyer/logex"
)

const (
	SlotSize = 8
)

var (
	ErrFileClosed        = logex.Define("file is closed")
	ErrFileInvalidBit    = logex.Define("invalid bit")
	ErrFileInvalidOffset = logex.Define("invalid offset")
)

type File struct {
	root      string
	bit       uint
	chunkSize int64

	closed int32
	slot   [SlotSize]*chunkctx
	m      sync.Mutex
}

func NewFile(path string) (*File, error) {
	return NewFileEx(path, 22)
}

func NewFileEx(root string, bit uint) (*File, error) {
	if bit > 32 {
		return nil, ErrFileInvalidBit.Trace(bit)
	}
	if err := os.MkdirAll(root, 0744); err != nil {
		return nil, logex.Trace(err)
	}

	root = filepath.Join(root)
	file := &File{
		root:      root,
		bit:       bit,
		chunkSize: 1 << bit,
	}
	return file, nil
}

func (f *File) Close() error {
	if !atomic.CompareAndSwapInt32(&f.closed, 0, 1) {
		return nil
	}
	f.m.Lock()
	defer f.m.Unlock()
	for idx, chunk := range f.slot {
		if chunk != nil {
			chunk.done()
			f.slot[idx] = nil
		}
	}
	return nil
}

func (f *File) getChunk(off int64, writeOp bool) (int64, *chunkctx, error) {
	if off < 0 {
		return -1, nil, ErrFileInvalidOffset.Trace()
	}
	chunkIdx := off >> f.bit

	f.m.Lock()
	chunk := f.slot[chunkIdx%SlotSize]

	// already opened
	if chunk != nil && chunk.idx == chunkIdx {
		chunk.add()
		f.m.Unlock()
		return chunkIdx, chunk, nil
	}

	newChunk, err := newChunkctx(f.root, chunkIdx, writeOp)
	if err != nil {
		f.m.Unlock()
		return chunkIdx, nil, logex.Trace(err)
	}

	newChunk.add()
	f.slot[chunkIdx%SlotSize] = newChunk
	f.m.Unlock()

	// clean old chunk
	chunk.done()
	return chunkIdx, newChunk, nil
}

func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	if atomic.LoadInt32(&f.closed) != 0 {
		return 0, ErrFileClosed.Trace()
	}

	chunkIdx, chunk, err := f.getChunk(off, true)
	if err != nil {
		return 0, logex.Trace(err)
	}
	// offset in chunk
	chunkOff := off & (f.chunkSize - 1)
	sizeLeft := f.chunkSize - chunkOff

	// size of chunk which available can fit the data
	if int64(len(b)) <= sizeLeft {
		n, err = chunk.WriteAt(b, chunkOff)
		if err != nil {
			err = logex.Trace(err)
		}
		chunk.done()
		return n, err
	}

	n, err = chunk.WriteAt(b[:sizeLeft], chunkOff)
	chunk.done()
	if err != nil {
		return n, logex.Trace(err)
	}

	n2, err := f.WriteAt(b[sizeLeft:], (chunkIdx+1)<<f.bit)
	if err != nil {
		err = logex.Trace(err)
	}
	return n + n2, err
}

func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	if atomic.LoadInt32(&f.closed) != 0 {
		return 0, ErrFileClosed.Trace()
	}
	chunkIdx, chunk, err := f.getChunk(off, false)
	if err != nil {
		return 0, logex.Trace(err)
	}

	chunkOff := off & (f.chunkSize - 1)
	sizeLeft := f.chunkSize - chunkOff

	if int64(len(b)) <= sizeLeft {
		n, err = chunk.ReadAt(b, chunkOff)
		chunk.done()
		if err != nil {
			err = logex.Trace(err)
		}
		return
	}

	n, err = chunk.ReadAt(b[:sizeLeft], chunkOff)
	chunk.done()
	if err != nil {
		return n, logex.Trace(err)
	}

	n2, err := f.ReadAt(b[sizeLeft:], (chunkIdx+1)<<f.bit)
	if err != nil {
		err = logex.Trace(err)
	}
	return n + n2, err
}

func (f *File) Delete(close bool) error {
	f.Close()

	if err := os.RemoveAll(f.root); err != nil {
		return logex.Trace(err)
	}
	if !close { // reopen
		os.MkdirAll(f.root, 0777)
		atomic.StoreInt32(&f.closed, 0)
	}
	return nil
}

// -----------------------------------------------------------------------------

type chunkctx struct {
	fd  *os.File
	idx int64
	ref int32
}

func newChunkctx(base string, idx int64, writeOp bool) (*chunkctx, error) {
	fp := filepath.Join(base, strconv.FormatInt(idx, 36))
	oflag := os.O_RDWR
	if writeOp {
		oflag |= os.O_CREATE
	}
	fd, err := os.OpenFile(fp, oflag, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, logex.Trace(io.EOF)
		}
		return nil, logex.Trace(err)
	}
	ctx := &chunkctx{
		fd:  fd,
		idx: idx,
		ref: 1,
	}
	return ctx, nil
}

func (f *chunkctx) add() {
	atomic.AddInt32(&f.ref, 1)
}

func (f *chunkctx) done() {
	if f == nil {
		return
	}
	if atomic.AddInt32(&f.ref, -1) == 0 {
		f.fd.Close()
	}
}

func (f *chunkctx) ReadAt(b []byte, off int64) (int, error) {
	return f.fd.ReadAt(b, off)
}

func (f *chunkctx) WriteAt(b []byte, off int64) (int, error) {
	return f.fd.WriteAt(b, off)
}
