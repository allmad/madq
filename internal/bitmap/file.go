package bitmap

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"gopkg.in/logex.v1"
)

const (
	CacheSize = 8
	ChunkBit  = 30
)

type fileCtx struct {
	*os.File
	ref int32
	idx int64
}

func (f *fileCtx) Acquire() {
	atomic.AddInt32(&f.ref, 1)
}

func (f *fileCtx) Release() {
	ref := atomic.AddInt32(&f.ref, -1)
	if ref == 0 {
		f.File.Close()
	}
}

// File abstract that there is a unlimit-sized file
type File struct {
	base  string
	cache [CacheSize]*fileCtx
	sync.RWMutex
}

func NewFile(path string) (*File, error) {
	err := os.MkdirAll(path, 0711)
	if err != nil {
		return nil, logex.Trace(err)
	}
	return &File{
		base: path + "/",
	}, nil
}

func (f *File) getFile(offset int64) (*fileCtx, error) {
	idx := offset >> ChunkBit
	name := strconv.FormatInt(idx, 36)
	cacheIdx := idx % CacheSize

	f.RLock()
	fctx := f.cache[cacheIdx]
	if fctx != nil && fctx.idx == idx {
		fctx.Acquire()
		f.RUnlock()
		return fctx, nil
	}
	f.RUnlock()

	of, err := os.OpenFile(f.base+name, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, logex.Trace(err)
	}
	newFctx := &fileCtx{of, 2, idx}

	f.Lock()
	fctx = f.cache[cacheIdx]
	f.cache[cacheIdx] = newFctx
	f.Unlock()

	if fctx != nil {
		fctx.Release()
	}

	return fctx, nil
}

func (f *File) WriteAt(buf []byte, at int64) (n int, err error) {

	return
}
