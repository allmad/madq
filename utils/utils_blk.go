package utils

import "gopkg.in/logex.v1"

var (
	ErrBlkFull = logex.Define("blk full")
)

type Blk struct {
	blk []byte
	off int
}

func NewBlk(size int) *Blk {
	return &Blk{
		blk: make([]byte, size),
	}
}

func (b *Blk) Skip(n int) {
	if n > 0 {
		b.off += n
	}
}

func (b *Blk) Len() int {
	return b.off
}

func (b *Blk) Remain() int {
	return len(b.blk) - b.off
}

func (b *Blk) Write(p []byte) (written int, err error) {
	if len(p) > len(b.blk)-b.off {
		return 0, ErrBlkFull.Trace()
	}

	n := copy(b.blk[b.off:], p)
	b.off += n
	return n, nil
}

func (b *Blk) Bytes() []byte {
	return b.blk[:b.off]
}
func (b *Blk) Reset() {
	b.off = 0
}
