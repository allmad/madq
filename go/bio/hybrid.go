package bio

import (
	"github.com/allmad/madq/go/common"
	"github.com/chzyer/logex"
)

type Hybrid struct {
	ReadWriterAt
	blksize int
	blkbit  uint
	lru     *common.LRUBytes
}

func NewHybrid(rw ReadWriterAt, blkbit uint) *Hybrid {
	return &Hybrid{
		ReadWriterAt: rw,
		blkbit:       blkbit,
		blksize:      1 << blkbit,
		lru:          common.NewLRUBytes(8, 1<<blkbit),
	}
}

func (h *Hybrid) ReadData(off int64, n int) ([]byte, error) {
	offblk := off & int64(h.blksize-1)
	if int(offblk)+n > h.blksize {
		buf := make([]byte, n)
		n, err := h.ReadWriterAt.ReadAt(buf, off)
		if err != nil {
			return nil, logex.Trace(err)
		}
		return buf[:n], nil
	}

	alignOff := off - offblk
	block := h.lru.Get(alignOff)
	ret, err := block.Get(h.ReadWriterAt, off, n)
	return ret, logex.Trace(err)
}
