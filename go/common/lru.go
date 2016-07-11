package common

import (
	"container/list"
	"io"
	"sync"

	"github.com/chzyer/logex"
)

type LRUItem struct {
	data   []byte
	offset int64
	guard  sync.RWMutex
}

func (l *LRUItem) Get(r io.ReaderAt, off int64, n int) ([]byte, error) {
	l.guard.RLock()
	size := int64(len(l.data))
	l.guard.RUnlock()

	if l.offset+size > off+int64(n) {
		return l.data[off-l.offset : off-l.offset+int64(n)], nil
	}

	l.guard.Lock()
	size = int64(len(l.data))
	if l.offset+size > off+int64(n) {
		l.guard.Unlock()
		return l.data[off-l.offset : off-l.offset+int64(n)], nil
	}

	// read
	l.data = l.data[:cap(l.data)]
	readbytes, err := r.ReadAt(l.data[size:], l.offset+size)
	if readbytes > 0 && logex.Equal(err, io.EOF) {
		err = nil
	}
	l.data = l.data[:size+int64(readbytes)]
	l.guard.Unlock()
	if err != nil {
		return nil, logex.Trace(err)
	}
	if len(l.data) < int(off-l.offset)+n {
		return nil, logex.Trace(io.EOF)
	}

	return l.data[off-l.offset : off-l.offset+int64(n)], nil
}

func (l *LRUItem) Len() int {
	return len(l.data)
}

type LRUBytes struct {
	size    int
	list    *list.List
	index   map[int64]*LRUItem
	blksize int
}

func NewLRUBytes(size int, block int) *LRUBytes {
	return &LRUBytes{
		size:    size,
		list:    list.New(),
		index:   make(map[int64]*LRUItem, size),
		blksize: block,
	}
}

func (l *LRUBytes) Get(off int64) *LRUItem {
	if ret := l.get(off); ret != nil {
		return ret
	}
	ret := l.add(off)
	return ret
}

func (l *LRUBytes) add(off int64) *LRUItem {
	ret := &LRUItem{
		data:   make([]byte, 0, l.blksize),
		offset: off,
	}
	l.list.PushFront(ret)
	l.index[off] = ret
	if l.list.Len() > l.size {
		item := l.list.Remove(l.list.Back()).(*LRUItem)
		delete(l.index, item.offset)
	}
	return ret
}

func (l *LRUBytes) get(off int64) *LRUItem {
	return l.index[off]
}
