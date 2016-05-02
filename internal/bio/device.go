package bio

import (
	"io"
	"sync"

	"github.com/chzyer/logex"
)

var (
	ErrDeviceWriteback   = logex.Define("write back is not allowed")
	ErrDeviceBufOverflow = logex.Define("buffer overflowed")
)

// buffered writer
type Device struct {
	raw RawDisker

	mutex sync.Mutex

	// buffer
	bufStart int64
	bufOff   int
	buf      [2 * (8 << 20)]byte
}

func NewDevice(raw RawDisker, offset int64) *Device {
	bd := &Device{
		raw:      raw,
		bufStart: offset,
	}
	return bd
}

func (d *Device) Buffered() int {
	d.mutex.Lock()
	ret := d.bufOff
	d.mutex.Unlock()
	return ret
}

func (d *Device) ReadAt(b []byte, off int64) (int, error) {
	d.mutex.Lock()
	n, err := d.readAtLocked(b, off)
	d.mutex.Unlock()
	return n, err
}

func (d *Device) readAtLocked(b []byte, off int64) (int, error) {
	if off+int64(len(b)) <= d.bufStart {
		// all we want is all on disk
		return d.raw.ReadAt(b, off)
	}

	if off >= d.bufStart {
		// all we want is all in buffer
		off := int(off - d.bufStart)
		var (
			n   int
			err error
		)
		if d.bufOff > off {
			n = copy(b, d.buf[off:d.bufOff])
		}
		if n == 0 {
			err = io.EOF
		}
		return n, err
	}

	// ok, it's some on disk and some in memory
	sizeOnDisk := int(d.bufStart - off)
	nDisk, err := d.raw.ReadAt(b[:sizeOnDisk], off)
	if err != nil {
		return nDisk, err
	}

	nMem := copy(b[sizeOnDisk:], d.buf[:d.bufOff])
	if nMem+nDisk == 0 {
		err = io.EOF
	}
	return nMem + nDisk, err
}

// write to buffer
func (d *Device) Write(b []byte) (int, error) {
	d.mutex.Lock()
	n, err := d.writeAtLocked(b, d.bufOff)
	isFull := d.isFullLocked()
	d.mutex.Unlock()
	if err != nil {
		return n, logex.Trace(err)
	}

	if isFull {
		err := d.Flush()
		if err != nil {
			logex.Error("flush error:", err)
		}
	}
	return n, err
}

func (d *Device) isFullLocked() bool {
	return d.bufOff >= len(d.buf)/2
}

func (d *Device) writeAtLocked(b []byte, off int) (int, error) {
	if off < 0 {
		return 0, ErrDeviceWriteback.Trace()
	} else if off > len(d.buf) {
		return 0, ErrDeviceBufOverflow.Trace()
	}
	copy(d.buf[off:], b)
	if off+len(b) > d.bufOff {
		d.bufOff = off + len(b)
	}
	return len(b), nil
}

func (d *Device) WriteAt(b []byte, off int64) (int, error) {
	d.mutex.Lock()
	n, err := d.writeAtLocked(b, int(off-d.bufStart))
	isFull := d.isFullLocked()
	d.mutex.Unlock()
	if isFull {
		err := d.Flush()
		if err != nil {
			logex.Error("flush error:", err)
		}
	}
	return n, err
}

func (d *Device) Flush() error {
	d.mutex.Lock()
	if d.bufOff == 0 {
		d.mutex.Unlock()
		return nil
	}

	n, err := d.raw.WriteAt(d.buf[:d.bufOff], d.bufStart)
	if err != nil {
		d.mutex.Unlock()
		return logex.Trace(err)
	}

	d.bufStart += int64(n)
	d.bufOff = 0
	d.mutex.Unlock()
	return nil
}
