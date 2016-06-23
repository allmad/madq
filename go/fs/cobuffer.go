package fs

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

var ErrSpaceNotEnough = fmt.Errorf("buffer space is not enough")

type Cobuffer struct {
	buffer    []byte
	offset    int32
	rw        sync.RWMutex
	maxSize   int
	flushChan chan struct{}

	writeChan     chan struct{}
	writeChanSent int32
}

func NewCobuffer(n int, maxSize int) *Cobuffer {
	return &Cobuffer{
		buffer:    make([]byte, n),
		flushChan: make(chan struct{}, 1),
		writeChan: make(chan struct{}, 1),
	}
}

func (c *Cobuffer) grow() bool {
	success := false
	c.rw.Lock()
	if len(c.buffer) >= c.maxSize {
		goto exit
	}
	c.buffer = append(c.buffer, 0)
	success = true

exit:
	c.rw.Unlock()
	return success
}

func (c *Cobuffer) Flush() {
	select {
	case c.flushChan <- struct{}{}:
	default:
	}
}

func (c *Cobuffer) IsWritten() <-chan struct{} {
	return c.writeChan
}

func (c *Cobuffer) IsFlush() <-chan struct{} {
	return c.flushChan
}

func (c *Cobuffer) GetData() []byte {
	c.rw.Lock()
	n := int(c.offset)
	buf := make([]byte, n)

	copy(buf, c.buffer)
	c.offset = 0

	atomic.StoreInt32(&c.writeChanSent, 0)
	c.rw.Unlock()
	return buf
}

func (c *Cobuffer) WriteData(b []byte) {
	for {
		if c.writeData(b) {
			return
		}
		if !c.grow() {
			select {
			case c.flushChan <- struct{}{}:
			default:
			}
			runtime.Gosched()
		}
	}
}

func (c *Cobuffer) writeData(b []byte) bool {
	success := false

	c.rw.RLock()

	newOff := atomic.AddInt32(&c.offset, int32(len(b)))
	if newOff >= int32(len(c.buffer)) {
		atomic.AddInt32(&c.offset, -int32(len(b)))
		goto exit
	}

	copy(c.buffer[newOff-int32(len(b)):newOff], b)
	success = true

	if atomic.CompareAndSwapInt32(&c.writeChanSent, 0, 1) {
		select {
		case c.writeChan <- struct{}{}:
		default:
		}
	}

exit:
	c.rw.RUnlock()
	return success
}

func (c *Cobuffer) Close() {
	close(c.flushChan)
	close(c.writeChan)
}
