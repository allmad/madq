package fs

import (
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
)

type FlushDelegate interface {
	ReadData(offset ShortAddr, n int) ([]byte, error)
	WriteData(offset ShortAddr, b []byte) error
}

type Flusher struct {
	flow     *flow.Flow
	interval time.Duration
	offset   int64 // point to the start of partial
	delegate FlushDelegate

	flushChan chan struct{}
	opChan    chan *flusherWriteOp
}

type FlusherConfig struct {
	Interval time.Duration
	Delegate FlushDelegate
	Offset   int64
}

func NewFlusher(f *flow.Flow, cfg *FlusherConfig) *Flusher {
	flusher := &Flusher{
		interval:  cfg.Interval,
		opChan:    make(chan *flusherWriteOp, 8),
		flushChan: make(chan struct{}, 1),
		offset:    cfg.Offset,
		delegate:  cfg.Delegate,
		flow:      f.Fork(1),
	}
	f.SetOnClose(flusher.Close)
	go flusher.loop()
	return flusher
}

func (f *Flusher) handleOpInPartialArea(dw *DiskWriter, op *flusherWriteOp) error {
	ino, idx, err := op.inoPool.RefPayloadBlock()
	if err != nil {
		return logex.Tracefmt(
			"error in fetch inode(%v): %v", op.inoPool.ino, err)
	}

	dataAddr := ShortAddr(f.getAddr(dw.Written()))
	blkSize := ino.GetBlockSize(idx)
	if blkSize > 0 {
		oldData, err := f.delegate.ReadData(ino.Offsets[idx], blkSize)
		if err != nil {
			return logex.Tracefmt(
				"error in readdata at %v: %v", ino.Offsets[idx], err)
		}
		dw.WriteBytes(oldData)
	}
	dw.WriteBytes(op.data)

	ino.SetOffset(idx, dataAddr, len(op.data))

	if len(op.tmpInodes) == 0 || op.tmpInodes[len(op.tmpInodes)-1] != ino {
		op.tmpInodes = append(op.tmpInodes, ino)
	}

	return nil
}

func (f *Flusher) Flush() {
	select {
	case f.flushChan <- struct{}{}:
	default:
	}
}

func (f *Flusher) getAddr(offset int64) Address {
	return Address(f.offset + offset)
}

func (f *Flusher) handleOpInDataArea(dw *DiskWriter, op *flusherWriteOp) error {
	var inos []*Inode
	var dataAddr ShortAddr

writePayload:
	ino, idx, err := op.inoPool.RefPayloadBlock()
	if err != nil {
		return logex.Tracefmt(
			"error in fetch inode(%v): %v", op.inoPool.ino, err)
	}

	dataAddr = ShortAddr(f.getAddr(dw.Written()))

	blkSize := ino.GetBlockSize(idx)
	if blkSize > 0 {
		oldData, err := f.delegate.ReadData(ino.Offsets[idx], blkSize)
		if err != nil {
			return logex.Tracefmt(
				"error in readdata at %v: %v", ino.Offsets[idx], err)
		}
		dw.WriteBytes(oldData)
	}

	if len(op.data) < BlockSize-blkSize {
		goto exit
	}

	dw.WriteBytes(op.data[:BlockSize-blkSize])
	op.data = op.data[BlockSize-blkSize:]

	ino.SetOffset(idx, dataAddr, BlockSize-blkSize)
	if len(inos) == 0 || inos[len(inos)-1] != ino {
		inos = append(inos, ino)
	}
	goto writePayload

exit:
	op.tmpInodes = inos
	return nil
}

func (f *Flusher) handleOps(data []byte, ops []*flusherWriteOp) int64 {
	// p: payload, ino: inode, b: block, pp: partial payload
	// | data area | partial area            |
	// | b1 | b2   | b3                      |
	// | p1 | p2   | pp1 + pp2 + ino1 + ino2 |
	// > how to fsck ? follow a MagicEOF
	dw := NewDiskWriter(data)

	// in data area
	for idx, op := range ops {
		dw.Mark()
		if err := f.handleOpInDataArea(dw, op); err != nil {
			dw.Reset()
			op.done <- logex.Trace(err)
			ops[idx] = nil
			continue
		}
	}

	// in partial area
	for idx, op := range ops {
		if op == nil {
			continue
		}
		if len(op.data) == 0 {
			continue
		}
		dw.Mark()
		if err := f.handleOpInPartialArea(dw, op); err != nil {
			dw.Reset()
			op.done <- logex.Trace(err)
			ops[idx] = nil
			continue
		}
	}

	// write inode
	for _, op := range ops {
		if op == nil {
			continue
		}
		for _, ino := range op.tmpInodes {
			inoAddr := f.getAddr(dw.Written())
			dw.WriteItem(ino)
			op.inoPool.OnFlush(ino, inoAddr)
		}
	}

	dw.WriteBytes(MagicEOF)
	return dw.Written()
}

func (f *Flusher) flush(fb *flushBuffer) {
	buffer := fb.alloc()
	written := f.handleOps(buffer, fb.ops())
	buffer = buffer[:written]

	// write to disk
flush:
	err := f.delegate.WriteData(ShortAddr(f.offset), buffer)
	if err != nil {
		logex.Error("error in write data, wait 1 sec:", err)
		switch f.flow.CloseOrWait(time.Second) {
		case flow.F_CLOSED:
			for _, op := range fb.ops() {
				op.done <- logex.Trace(err)
			}
			fb.reset()
			return
		case flow.F_TIMEOUT:
			goto flush
		}
	}

	f.offset += int64(len(buffer))
	for _, op := range fb.ops() {
		op.done <- nil
	}
	fb.reset()
}

func (f *Flusher) loop() {
	defer f.flow.Done()

	var (
		fb    flushBuffer
		timer = time.NewTimer(0)
	)
	timer.Stop()

loop:
	for {
		select {
		case op := <-f.opChan:
			fb.addOp(op)
			timer.Reset(f.interval)

		buffering:
			for {
				select {
				case <-timer.C:
					break buffering
				case <-f.flushChan:
					break buffering
				case op := <-f.opChan:
					fb.addOp(op)
				}
			}

			f.flush(&fb)
		case <-f.flow.IsClose():
			break loop
		}
	}
}

type flusherWriteOp struct {
	tmpInodes []*Inode

	inoPool *InodePool
	done    chan error
	data    []byte
}

func (f *Flusher) WriteByInode(inoPool *InodePool, data []byte, done chan error) {
	f.opChan <- &flusherWriteOp{inoPool: inoPool, data: data, done: done}
}

func (f *Flusher) Close() {
	if !f.flow.MarkExit() {
		return
	}

	f.flow.Close()
	close(f.opChan)

	var fb flushBuffer
	for op := range f.opChan {
		fb.addOp(op)
	}
	f.flush(&fb)
}

type flushBuffer struct {
	bufferingOps  []*flusherWriteOp
	bufferingSize int
	buffer        []byte
}

func (f *flushBuffer) addOp(op *flusherWriteOp) {
	f.bufferingOps = append(f.bufferingOps, op)
	f.bufferingSize += len(op.data) + GetBlockCnt(len(op.data))*InodeSize
}

func (f *flushBuffer) alloc() []byte {
	f.bufferingSize += MagicSize // MagicEOF
	f.buffer = MakeRoom(f.buffer, f.bufferingSize)
	return f.buffer
}

func (f *flushBuffer) ops() []*flusherWriteOp {
	return f.bufferingOps
}

func (f *flushBuffer) reset() {
	f.bufferingOps = f.bufferingOps[:0]
	f.bufferingSize = 0
	f.buffer = f.buffer[:0]
}
