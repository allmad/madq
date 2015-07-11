package bitmap

import (
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"gopkg.in/logex.v1"
)

const (
	CacheSize = 8
	ChunkBit  = 30
	ChunkSize = 1 << ChunkBit
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
	fctx = &fileCtx{of, 2, idx}

	f.Lock()
	oldFctx := f.cache[cacheIdx]
	f.cache[cacheIdx] = fctx
	f.Unlock()

	if oldFctx != nil {
		oldFctx.Release()
	}

	return fctx, nil
}

func (f *File) WriteAt(buf []byte, at int64) (n int, err error) {
	fctx, err := f.getFile(at)
	if err != nil {
		return 0, logex.Trace(err)
	}
	defer fctx.Release()

	chunkOffset := at - ((at >> ChunkBit) << ChunkBit)
	sizeLeft := ChunkSize - chunkOffset
	if sizeLeft > int64(len(buf)) {
		return fctx.WriteAt(buf, chunkOffset)
	}

	n, err = fctx.WriteAt(buf[:sizeLeft], at>>ChunkBit)
	if err != nil {
		return n, logex.Trace(err)
	}

	return f.WriteAt(buf[sizeLeft:], at+sizeLeft)
}

func (f *File) ReadAt(buf []byte, at int64) (n int, err error) {
	fctx, err := f.getFile(at)
	if err != nil {
		return 0, logex.Trace(err)
	}
	defer fctx.Release()

	chunkOffset := at - ((at >> ChunkBit) << ChunkBit)
	sizeLeft := ChunkSize - chunkOffset

	n, err = fctx.ReadAt(buf, chunkOffset)
	if err != nil {
		return n, logex.Trace(err, chunkOffset)
	}
	if sizeLeft > int64(len(buf)) {
		return n, nil
	}

	nNew, err := f.ReadAt(buf[n:], at+int64(n))
	n += nNew
	return n, logex.Trace(err)
}

func (f *File) Close() {
	f.Lock()
	defer f.Unlock()

	for i := 0; i < CacheSize; i++ {
		if f.cache[i] == nil {
			continue
		}
		f.cache[i].Release()
		f.cache[i] = nil
	}
}

func (f *File) Size() int64 {
	fdir, err := os.Open(f.base)
	if err != nil {
		logex.Error(err)
		return 0
	}
	names, err := fdir.Readdirnames(-1)
	if err != nil {
		fdir.Close()
		logex.Error(err)
		return 0
	}
	fdir.Close()
	if len(names) == 0 {
		return 0
	}
	sort.Strings(names)

	var (
		lastFile string
		chunkIdx int64
	)
	for i := len(names) - 1; i >= 0; i-- {
		chunkIdx, err = strconv.ParseInt(names[i], 36, 64)
		if err != nil {
			continue
		}
		lastFile = names[i]
		break
	}
	if lastFile == "" {
		return 0
	}
	info, err := os.Stat(f.base + lastFile)
	if err != nil {
		return 0
	}

	return chunkIdx<<ChunkBit + info.Size()
}
