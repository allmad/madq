package fs

import (
	"sync"
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

	flushChan   chan struct{}
	flushWaiter sync.WaitGroup

	opChan chan *flusherWriteOp
}

type FlusherConfig struct {
	Interval time.Duration
	Delegate FlushDelegate
	Offset   int64
}

func NewFlusher(f *flow.Flow, cfg *FlusherConfig) *Flusher {
	flusher := &Flusher{
		interval:  cfg.Interval,
		opChan:    make(chan *flusherWriteOp, 100),
		flushChan: make(chan struct{}, 1),
		offset:    cfg.Offset,
		delegate:  cfg.Delegate,
		flow:      f.Fork(1),
	}
	f.SetOnClose(flusher.Close)
	go flusher.loop()
	return flusher
}

func (f *Flusher) handleOpInPartialArea(dw *DiskWriter, op *flushItem) error {
	ino, idx, err := op.inoPool.RefPayloadBlock()
	if err != nil {
		return logex.Tracefmt(
			"error in fetch inode(%v): %v", op.inoPool.ino, err)
	}

	blkSize := ino.GetBlockSize(idx)
	dataAddr := ShortAddr(f.getAddr(dw.Written()))

	if blkSize > 0 {
		// ino.Offsets[idx] maybe in memory
		oldData, err := f.delegate.ReadData(ino.Offsets[idx], blkSize)
		if err != nil {
			return logex.Tracefmt(
				"error in readdata at %v(%v): %v",
				ino.Offsets[idx], blkSize, err)
		}
		dw.WriteBytes(oldData)
		Stat.Flusher.BlockCopy.AddInt(len(oldData))
	}
	length := op.data.Len()
	op.data.WriteData(dw, -1)
	ino.SetOffset(idx, dataAddr, length)

	if len(op.tmpInodes) == 0 || op.tmpInodes[len(op.tmpInodes)-1] != ino {
		op.tmpInodes = append(op.tmpInodes, ino)
	}

	return nil
}

func (f *Flusher) Flush() {
	f.flushWaiter.Add(1)
	select {
	case f.flushChan <- struct{}{}:
		f.flushWaiter.Wait()
	default:
		f.flushWaiter.Done()
	}
}

func (f *Flusher) getAddr(offset int64) Address {
	return Address(f.offset + offset)
}

func (f *Flusher) handleOpInDataArea(dw *DiskWriter, op *flushItem) error {
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
	if op.data.Len() < BlockSize-blkSize {
		goto exit
	}

	if blkSize > 0 {
		oldData, err := f.delegate.ReadData(ino.Offsets[idx], blkSize)
		if err != nil {
			return logex.Tracefmt(
				"error in readdata at %v: %v", ino.Offsets[idx], err)
		}
		dw.WriteBytes(oldData)
		Stat.Flusher.BlockCopy.AddInt(len(oldData))
	}

	op.data.WriteData(dw, BlockSize-blkSize)

	ino.SetOffset(idx, dataAddr, BlockSize-blkSize)
	if len(inos) == 0 || inos[len(inos)-1] != ino {
		inos = append(inos, ino)
	}
	goto writePayload

exit:
	op.tmpInodes = inos
	return nil
}

func (f *Flusher) handleOps(data []byte, ops []*flushItem) int64 {
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
			op.sendDone(logex.Trace(err))
			ops[idx] = nil
			continue
		}
	}

	// in partial area
	for idx, op := range ops {
		if op == nil {
			continue
		}
		if op.data.Len() == 0 {
			continue
		}
		dw.Mark()
		if err := f.handleOpInPartialArea(dw, op); err != nil {
			dw.Reset()
			op.sendDone(logex.Trace(err))
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

	// send reply to ops in flush()

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
				if op == nil {
					continue
				}
				op.sendDone(logex.Trace(err))
			}
			fb.reset()
			return
		case flow.F_TIMEOUT:
			goto flush
		}
	}

	f.offset += int64(len(buffer))
	for _, op := range fb.ops() {
		if op == nil {
			continue
		}
		op.sendDone(nil)
	}
	fb.reset()
}

func (f *Flusher) loop() {
	defer f.flow.Done()

	var (
		fb    flushBuffer
		timer = time.NewTimer(0)
	)
	fb.init()
	timer.Stop()
	wantFlush := false

loop:
	for {
		select {
		case op := <-f.opChan:
			fb.addOp(op)
			timer.Reset(f.interval)

			if !wantFlush {
			buffering:
				for {
					select {
					case <-timer.C:
						break buffering
					case <-f.flushChan:
						wantFlush = true
						break buffering
					case op := <-f.opChan:
						if !fb.addOp(op) {
							break buffering
						}
					}
				}
			}

			f.flush(&fb)
			if wantFlush {
				f.flushWaiter.Done()
				wantFlush = false
			}
		case <-f.flushChan:
			if len(f.opChan) != 0 {
				wantFlush = true
			} else {
				f.flushWaiter.Done()
			}

		case <-f.flow.IsClose():
			break loop
		}
	}
}

type flusherWriteOp struct {
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
	fb.init()
	for op := range f.opChan {
		fb.addOp(op)
	}
	f.flush(&fb)
}

type flushItem struct {
	tmpInodes []*Inode
	inoPool   *InodePool
	done      []chan error
	data      *DataSlice
}

func (f *flushItem) sendDone(err error) {
	for _, d := range f.done {
		d <- err
	}
}

type flushBuffer struct {
	bufferingOps  []*flushItem
	bufferingSize int
	buffer        []byte
}

func (f *flushBuffer) init() {
	f.buffer = make([]byte, 4<<20)
}

func (f *flushBuffer) findOp(op *flusherWriteOp) int {
	for idx, bop := range f.bufferingOps {
		if bop.inoPool == op.inoPool {
			return idx
		}
	}
	return -1
}

// return false to flush
func (f *flushBuffer) addOp(op *flusherWriteOp) bool {
	// if multiple op belong to same file?
	idx := f.findOp(op)
	if idx >= 0 {
		bop := f.bufferingOps[idx]
		bop.data.Append(op.data)
		bop.done = append(bop.done, op.done)
	} else {
		f.bufferingOps = append(f.bufferingOps, &flushItem{
			inoPool: op.inoPool,
			done:    []chan error{op.done},
			data:    NewDataSlice(op.data),
		})
	}

	ino, err := op.inoPool.GetLastest()
	if err != nil {
		panic("can not get lastest")
	}

	// calculate the copy of partial data
	f.bufferingSize += len(op.data) +
		CalNeedInodeCnt(ino, len(op.data))*InodeSize +
		int(ino.Size)&(BlockSize-1)

	if f.bufferingSize >= 20<<20 {
		return false
	}
	return true
}

func (f *flushBuffer) alloc() []byte {
	f.bufferingSize += MagicSize // MagicEOF
	f.buffer = MakeRoom(f.buffer, f.bufferingSize)
	return f.buffer
}

func (f *flushBuffer) ops() []*flushItem {
	return f.bufferingOps
}

func (f *flushBuffer) reset() {
	f.bufferingOps = f.bufferingOps[:0]
	f.bufferingSize = 0
	f.buffer = f.buffer[:0]
}

// -----------------------------------------------------------------------------
type DataSlice struct {
	data   [][]byte
	length int
	offset int
}

func NewDataSlice(d []byte) *DataSlice {
	return &DataSlice{
		data:   [][]byte{d},
		length: len(d),
	}
}

func (f *DataSlice) Append(b []byte) {
	f.data = append(f.data, b)
	f.length += len(b)
}

func (f *DataSlice) WriteData(dw *DiskWriter, n int) {
	if n == -1 {
		n = f.length
	}

	for i := f.offset; i < len(f.data) && n > 0; i++ {
		f.offset = i
		if n > len(f.data[i]) {
			dw.WriteBytes(f.data[i])
			f.length -= len(f.data[i])
			n -= len(f.data[i])
			f.data[i] = nil
		} else {
			dw.WriteBytes(f.data[i][:n])
			f.length -= n
			f.data[i] = f.data[i][n:]
			n = 0
		}
	}
}

func (f *DataSlice) Len() int {
	return f.length
}
