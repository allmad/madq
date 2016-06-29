package fs

import (
	"fmt"
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
	writeTime     time.Time
	waiter        sync.WaitGroup
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

	now := time.Now()
	c.rw.Lock()
	if len(c.buffer) >= c.maxSize {
		c.rw.Unlock()
		return false
	}
	c.buffer = append(c.buffer, 0)
	c.buffer = c.buffer[:cap(c.buffer)]
	c.rw.Unlock()
	Stat.Cobuffer.Grow.AddNow(now)
	return true
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
	c.waiter.Add(1)
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
	Stat.Cobuffer.GetData.Lock.AddNow(now)
	n := int(c.offset)
	if len(buffer) < n {
		c.rw.Unlock()
		return n
	}

	now = time.Now()
	copy(buffer[:n], c.buffer)
	c.offset = 0
	Stat.Cobuffer.GetData.Copy.AddNow(now)
	Stat.Cobuffer.GetData.Size.Add(int64(n))

	Stat.Cobuffer.FlushDelay.AddNow(c.wantFlushTime)
	c.wantFlushTime = time.Now()

	atomic.StoreInt32(&c.writeChanSent, 0)
	if atomic.SwapInt32(&c.flushChanSent, 0) == 1 {
		c.waiter.Done()
	}
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
			c.waiter.Wait()
		}
	}
}

func (c *Cobuffer) writeData(b []byte) bool {
	if c.isWantFlush() {
		return false
	}

	now := time.Now()

	c.rw.RLock()

	newOff := atomic.AddInt32(&c.offset, int32(len(b)))
	if newOff >= int32(len(c.buffer)) {
		atomic.AddInt32(&c.offset, -int32(len(b)))
		c.rw.RUnlock()
		return false
	}

	copy(c.buffer[newOff-int32(len(b)):newOff], b)

	if atomic.CompareAndSwapInt32(&c.writeChanSent, 0, 1) {
		c.writeTime = time.Now()
		select {
		case c.writeChan <- struct{}{}:
		default:
		}
	}

	if int(newOff) > c.maxSize/2 {
		if !c.isWantFlush() {
			// println("cobuffer: need flush")
			Stat.Cobuffer.NotifyFlushByWrite.Hit()
			Stat.Cobuffer.FullTime.AddNow(c.writeTime)
			c.Flush()
		}
	} else {
		Stat.Cobuffer.NotifyFlushByWrite.Miss()
	}

	c.rw.RUnlock()
	Stat.Cobuffer.WriteTime.AddNow(now)
	return true
}

func (c *Cobuffer) Close() {
	close(c.flushChan)
	close(c.writeChan)
}
