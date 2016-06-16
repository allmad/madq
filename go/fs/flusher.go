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

	opChan chan *flusherWriteOp
}

func NewFlusher(f *flow.Flow, interval time.Duration, delegate FlushDelegate) *Flusher {
	flusher := &Flusher{
		interval: interval,
		opChan:   make(chan *flusherWriteOp, 8),
		delegate: delegate,
		flow:     f.Fork(1),
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
	inoAddr := f.getAddr(dw.Written())
	dw.WriteItem(ino)
	op.inoPool.OnFlush(ino, inoAddr)
	return nil
}

func (f *Flusher) getAddr(offset int64) Address {
	return Address(f.offset + offset)
}

func (f *Flusher) handleOpInDataArea(dw *DiskWriter, op *flusherWriteOp) error {
	var inos []*Inode

writePayload:
	ino, idx, err := op.inoPool.RefPayloadBlock()
	if err != nil {
		return logex.Tracefmt(
			"error in fetch inode(%v): %v", op.inoPool.ino, err)
	}

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
		goto writeInode
	}

	dw.WriteBytes(op.data[:BlockSize-blkSize])
	op.data = op.data[BlockSize-blkSize:]

	ino.SetOffset(idx, ShortAddr(f.offset+dw.Written()), BlockSize-blkSize)
	if len(inos) == 0 || inos[len(inos)-1] != ino {
		inos = append(inos, ino)
	}
	goto writePayload

writeInode:
	for _, ino := range inos {
		inoAddr := f.getAddr(dw.Written())
		dw.WriteItem(ino)
		op.inoPool.OnFlush(ino, inoAddr)
	}

	return nil
}

func (f *Flusher) handleOps(data []byte, ops []*flusherWriteOp) int64 {
	// p: payload, ino: inode, b: block, pp: partial payload
	// | data area                           | partial area            |
	// | b1 | b2 | b3                        | b4                      |
	// | p1 | p2 | ino1(p1) + ino2(p2) + EOF | pp1 + pp2 + ino1 + ino2 |
	// inode always in partial area until they can fill one block (256)
	// every write will overwrite partial area
	// > how to fsck ? follow a MagicEOF
	dw := NewDiskWriter(data)

	// in data area
	for _, op := range ops {
		if err := f.handleOpInDataArea(dw, op); err != nil {
			logex.Error(err)
			continue
		}
	}

	// in partial area
	for _, op := range ops {
		// ignore if we only want to write inode without payload
		// if len(op.data) == 0 {
		//	continue
		// }
		if err := f.handleOpInPartialArea(dw, op); err != nil {
			logex.Error(err)
			continue
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
		time.Sleep(time.Second)
		goto flush
	}

	f.offset += int64(len(buffer))
	// write partialcp

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
	inoPool *InodePool
	data    []byte
}

func (f *Flusher) WriteByInode(inoPool *InodePool, data []byte) {
	f.opChan <- &flusherWriteOp{inoPool, data}
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
	f.bufferingSize += FloorBlk(len(op.data)) + 2*InodeSize
}

func (f *flushBuffer) alloc() []byte {
	f.bufferingSize += 2 * MagicSize // MagicEOF
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
