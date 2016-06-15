package bio

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/chzyer/flow"
)

type DeviceMgr struct {
	flow *flow.Flow

	dev    *Device
	wg     sync.WaitGroup
	setOff *int64
	offset int64

	flushChan     chan struct{}
	needFlushChan chan struct{}

	m sync.Mutex
}

func NewDeviceMgr(f *flow.Flow, d *Device, setOff *int64) *DeviceMgr {
	dm := &DeviceMgr{
		dev:           d,
		setOff:        setOff,
		offset:        d.Offset(),
		flushChan:     make(chan struct{}),
		needFlushChan: make(chan struct{}, 1),
	}
	f.ForkTo(&dm.flow, dm.Close)
	return dm
}

func (d *DeviceMgr) loop() {
	d.flow.Add(1)
	defer d.flow.DoneAndClose()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-ticker.C:
			d.Flush()
		case <-d.needFlushChan:
			d.Flush()
		case <-d.flow.IsClose():
			break loop
		}
	}
}

func (d *DeviceMgr) Close() {
	d.flow.Close()
	d.Flush()
}

// when client received signal, it's time to Done writing
// and try GetFlushNotify() to get a new one
func (d *DeviceMgr) GetFlushNotify() chan struct{} {
	d.m.Lock()
	ret := d.flushChan
	d.wg.Add(1)
	d.m.Unlock()
	return ret
}

func (d *DeviceMgr) Malloc(n int) int64 {
	d.m.Lock()
	d.m.Unlock()

	ret := atomic.AddInt64(&d.offset, int64(n))
	if ret > d.dev.FlushSize() {
		select {
		case d.needFlushChan <- struct{}{}:
		default:
		}
	}
	return ret - int64(n)
}

type DeviceWriter struct {
	*Writer
	d   *DeviceMgr
	off int64
}

func (d *DeviceWriter) Offset() int64 {
	return d.off
}

func (d *DeviceMgr) MallocWriter(n int) *DeviceWriter {
	off := d.Malloc(n)
	return &DeviceWriter{
		Writer: d.dev.GetWriter(off, n),
		d:      d,
		off:    off,
	}
}

func (d *DeviceMgr) WriteDisk(off int64, disk Diskable) {
	disk.WriteDisk(d.dev.GetWriter(off, disk.Size()))
}

func (d *DeviceMgr) Done() {
	d.wg.Done()
}

func (d *DeviceMgr) Flush() error {
	d.m.Lock()
	defer d.m.Unlock()

	// notify all done with writing
	ch := d.flushChan
	d.flushChan = make(chan struct{})
	close(ch)

	d.wg.Wait()
	if err := d.dev.Flush(); err != nil {
		return err
	}
	atomic.StoreInt64(d.setOff, d.dev.Offset())
	return nil
}

func (d *DeviceMgr) Raw() RawDisker {
	return d.dev.Raw()
}

func (d *DeviceMgr) ReadAt(b []byte, off int64) (int, error) {
	return d.dev.ReadAt(b, off)
}
