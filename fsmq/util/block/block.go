package block

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"gopkg.in/logex.v1"
)

const (
	DefaultBit = 22
	MaxBit     = 32
	SlotSize   = 8
)

var (
	ErrInvalidOffset = logex.Define("block: invalid offset")
	ErrBlockClosed   = logex.Define("block: closed")
	ErrInvalidBit    = logex.Define("block: invalid bit")
)

type Instance struct {
	root      string
	slot      [SlotSize]*filectx
	bit       uint
	chunkSize int64
	closed    int32
	flag      int
	wg        sync.WaitGroup
	sync.Mutex
}

func New(path string, flag int, bit uint) (*Instance, error) {
	if bit > MaxBit {
		return nil, ErrInvalidBit
	}
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, logex.Trace(err)
	}
	sp := string([]rune{filepath.Separator})
	if !strings.HasSuffix(path, sp) {
		path += sp
	}
	if flag == 0 {
		flag = os.O_RDWR
	}
	return &Instance{
		root:      path,
		bit:       bit,
		flag:      flag,
		chunkSize: 1 << bit,
	}, nil
}

func (i *Instance) getfile(off int64) (int64, *filectx, error) {
	if off < 0 {
		return -1, nil, ErrInvalidOffset
	}
	idx := off >> i.bit

	i.Lock()
	fctx := i.slot[idx%SlotSize]
	if fctx != nil && fctx.idx == idx {
		fctx.add()
		i.Unlock()
		return idx, fctx, nil
	}

	newFctx, err := newFilectx(i.root, idx, i.flag, &i.wg)
	if err != nil {
		i.Unlock()
		return idx, nil, logex.Trace(err)
	}
	i.slot[idx%SlotSize] = newFctx
	i.Unlock()
	fctx.done()
	newFctx.add()
	return idx, newFctx, nil
}

func (i *Instance) ReadAt(p []byte, off int64) (n int, err error) {
	if atomic.LoadInt32(&i.closed) != 0 {
		return 0, ErrBlockClosed
	}
	idx, fctx, err := i.getfile(off)
	if err != nil {
		return 0, logex.Trace(err)
	}

	chunkOff := off & (i.chunkSize - 1)
	sizeLeft := int64(i.chunkSize) - chunkOff

	if int64(len(p)) <= sizeLeft {
		n, err = fctx.ReadAt(p, chunkOff)
		fctx.done()
		if err != nil {
			err = logex.Trace(err)
		}
		return
	}

	n, err = fctx.ReadAt(p[:sizeLeft], chunkOff)
	fctx.done()
	if err != nil {
		return n, logex.Trace(err)
	}

	n2, err := i.ReadAt(p[sizeLeft:], (idx+1)<<i.bit)
	if err != nil {
		err = logex.Trace(err)
	}
	return n + n2, err
}

func (i *Instance) WriteAt(p []byte, off int64) (n int, err error) {
	if atomic.LoadInt32(&i.closed) != 0 {
		return 0, ErrBlockClosed
	}

	idx, fctx, err := i.getfile(off)
	if err != nil {
		return 0, logex.Trace(err)
	}
	chunkOff := off & (i.chunkSize - 1)
	sizeLeft := int64(i.chunkSize) - chunkOff

	if int64(len(p)) <= sizeLeft {
		n, err = fctx.WriteAt(p, chunkOff)
		fctx.done()
		if err != nil {
			err = logex.Trace(err)
		}
		return n, err
	}

	n, err = fctx.WriteAt(p[:sizeLeft], chunkOff)
	fctx.done()
	if err != nil {
		return n, logex.Trace(err)
	}

	n2, err := i.WriteAt(p[sizeLeft:], (idx+1)<<i.bit)
	if err != nil {
		err = logex.Trace(err)
	}
	return n + n2, err
}

func (i *Instance) Delete(close bool) error {
	if !i.Close() {
		return nil
	}
	if err := os.RemoveAll(i.root); err != nil {
		return logex.Trace(err)
	}
	if !close { // reopen
		os.MkdirAll(i.root, 0777)
		atomic.SwapInt32(&i.closed, 0)
	}
	return nil
}

func (i *Instance) Close() bool {
	if atomic.SwapInt32(&i.closed, 1) != 0 {
		return false
	}
	i.Lock()
	for idx, fc := range i.slot {
		fc.done()
		i.slot[idx] = nil
	}
	i.Unlock()
	return true
}

type filectx struct {
	*os.File
	idx int64
	ref int32
	wg  *sync.WaitGroup
}

func newFilectx(base string, idx int64, flag int, wg *sync.WaitGroup) (*filectx, error) {
	of, err := os.OpenFile(base+strconv.FormatInt(idx, 36), os.O_CREATE|flag, 0644)
	if err != nil {
		return nil, logex.Trace(err)
	}
	wg.Add(1)
	return &filectx{
		File: of,
		idx:  idx,
		ref:  1,
		wg:   wg,
	}, nil
}

func (f *filectx) add() {
	atomic.AddInt32(&f.ref, 1)
	f.wg.Add(1)
}

func (f *filectx) done() {
	if f == nil {
		return
	}
	f.wg.Done()
	if atomic.AddInt32(&f.ref, -1) == 0 {
		f.Close()
	}
}
