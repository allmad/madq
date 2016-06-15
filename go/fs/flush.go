package fs

import (
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
)

type Flusher struct {
	flow     *flow.Flow
	interval time.Duration
	offset   int64 // point to the start of partial

	opChan chan *flusherWriteOp
}

func NewFlusher(f *flow.Flow, interval time.Duration) *Flusher {
	flusher := &Flusher{
		interval: interval,
		opChan:   make(chan *flusherWriteOp, 8),
	}
	f.ForkTo(&flusher.flow, flusher.Close)
	go flusher.loop()
	return flusher
}

func (f *Flusher) handleOps(data []byte, ops []*flusherWriteOp) error {
	// p: payload, ino: inode, b: block, pp: partial payload
	// | data area                           | partial area            |
	// | b1 | b2 | b3                        | b4                      |
	// | p1 | p2 | ino1(p1) + ino2(p2) + EOF | pp1 + pp2 + ino1 + ino2 |
	// inode always in partial area until they can fill one block (256)
	// every write will overwrite partial area
	// > how to fsck ? follow a MagicEOF

	offset := int64(0)
	// in data area
	for _, op := range ops {

		ino, idx, err := op.inoPool.RefPayloadBlock()
		if err != nil {
			logex.Error("error in fetch inode! ignore it:", err)
			continue
		}
		blkSize := ino.GetBlockSize(idx)

		for len(op.data) >= BlockSize {
			n := copy(data[offset:], op.data[:BlockSize])
			op.data = op.data[BlockSize:]

			ino.Offsets[idx] = ShortAddr(f.offset + offset)
			ino.Size += BlockSize
			offset += int64(n)
		}
	}

	return nil
}

func (f *Flusher) loop() {
	f.flow.Add(1)
	defer f.flow.DoneAndClose()

	var (
		bufferingOps  []*flusherWriteOp
		bufferingSize int
		buffer        []byte

		timer = time.NewTimer(0)
	)
	timer.Stop()

loop:
	for {
		select {
		case op := <-f.opChan:
			bufferingOps = append(bufferingOps, op)
			bufferingSize += len(op.data)
			timer.Reset(f.interval)

		buffering:
			for {
				select {
				case <-timer.C:
					break buffering
				case op := <-f.opChan:
					bufferingOps = append(bufferingOps, op)
					bufferingSize += len(op.data)
				}
			}

			buffer = MakeRoom(buffer, bufferingSize)
			f.handleOps(buffer, bufferingOps)

			bufferingOps = bufferingOps[:0]
			bufferingSize = 0
			buffer = buffer[:0]
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
	// flush
	f.flow.Close()
}
