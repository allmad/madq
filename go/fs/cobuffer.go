package fs

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var ErrSpaceNotEnough = fmt.Errorf("buffer space is not enough")

type Cobuffer struct {
	buffer        []byte
	offset        int32
	rw            sync.RWMutex
	maxSize       int
	flushChan     chan struct{}
	flushChanSent int32
	wantFlushTime time.Time

	writeChan     chan struct{}
	writeChanSent int32
}

func NewCobuffer(n int, maxSize int) *Cobuffer {
	return &Cobuffer{
		buffer:    make([]byte, n),
		maxSize:   maxSize,
		flushChan: make(chan struct{}, 1),
		writeChan: make(chan struct{}, 1),
	}
}

func (c *Cobuffer) isWantFlush() bool {
	return atomic.LoadInt32(&c.flushChanSent) == 1
}

func (c *Cobuffer) grow() bool {
	if c.isWantFlush() {
		return false
	}

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
	if !atomic.CompareAndSwapInt32(&c.flushChanSent, 0, 1) {
		return
	}
	c.wantFlushTime = time.Now()
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

func (c *Cobuffer) GetData(buffer []byte) int {
	now := time.Now()
	c.rw.Lock()
	Stat.Cobuffer.GetDataLock.AddNow(now)
	now = time.Now()
	n := int(c.offset)
	if len(buffer) < n {
		c.rw.Unlock()
		return n
	}

	copy(buffer[:n], c.buffer)
	c.offset = 0

	Stat.Cobuffer.GetData.AddNow(now)
	Stat.Cobuffer.FlushDelay.AddNow(c.wantFlushTime)
	c.wantFlushTime = time.Now()

	atomic.StoreInt32(&c.writeChanSent, 0)
	atomic.StoreInt32(&c.flushChanSent, 0)
	c.rw.Unlock()
	return n
}

func (c *Cobuffer) WriteData(b []byte) {
	tryTime := 0
	for {
		if c.writeData(b) {
			Stat.Cobuffer.Trytime.HitN(tryTime)
			return
		}
		tryTime++
		if !c.grow() {
			c.Flush()
			runtime.Gosched()
		}
	}
}

func (c *Cobuffer) writeData(b []byte) bool {
	if c.isWantFlush() { // avoid RLock
		return false
	}

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

	Stat.Cobuffer.NotifyFlushByWrite.HitIf(int(newOff) > c.maxSize/2)
	if int(newOff) > c.maxSize/2 {
		c.Flush()
	}

exit:
	c.rw.RUnlock()
	return success
}

func (c *Cobuffer) Close() {
	close(c.flushChan)
	close(c.writeChan)
}
