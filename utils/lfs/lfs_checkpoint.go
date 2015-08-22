package lfs

import (
	"bytes"
	"encoding/gob"
	"io"
	"sync"

	"github.com/chzyer/muxque/utils"
	"github.com/chzyer/muxque/utils/bitmap"

	"gopkg.in/logex.v1"
)

const (
	MagicByte, MagicByteV2 byte = 0x9b, 0x90
)

var (
	MagicBytes = []byte{MagicByte, MagicByteV2}
)

type checkPoint struct {
	Data map[string]int64

	blkOff  int64
	blkBit  uint
	blkSize int
	blk     []byte

	sync.Mutex
}

func newCheckPoint(blkBit uint, r *bitmap.File) *checkPoint {
	cp := new(checkPoint)
	cp.blkBit = blkBit
	cp.blkSize = 1 << blkBit
	cp.blk = make([]byte, cp.blkSize)
	if err := cp.Restore(r); err != nil {
		cp.Data = make(map[string]int64)
	}
	return cp
}

func (c *checkPoint) Restore(r *bitmap.File) (err error) {
	size := r.Size() // make sure is blksize-round
	off := size - (size & int64(c.blkSize-1))
	buf := make([]byte, 2)
	for {
		if off == 0 {
			break
		}
		off -= int64(c.blkSize)
		_, err = r.ReadAt(buf, off)
		if err != nil {
			return
		}
		if !bytes.Equal(buf, MagicBytes) {
			continue
		}

		bio := utils.NewBufio(utils.NewReader(r, off+2))
		if err = gob.NewDecoder(bio).Decode(&c.Data); err != nil {
			logex.Error(err)
			return
		}

		off = bio.Offset(-1)
		break
	}

	// move off to next blk
	off = c.calFloor(off)

	if c.Data == nil {
		c.Data = make(map[string]int64)
	}
	updated := false
	if off < size {
		logex.Info("trying to resore ino into checkpoint")
	}
	bio := utils.NewBufio(utils.NewReader(r, off))
	for off < size {
		bio.Offset(off)
		ino, err := ReadInode(bio, c.blkBit)
		if err != nil {
			if logex.Equal(err, io.EOF) {
				break
			}
			off += int64(c.blkSize)
			continue
		}

		updated = true
		c.Data[ino.Name.String()] = off
		off = c.calFloor(bio.Offset(-1))
	}

	if updated {
		err = logex.Trace(c.Save(utils.NewWriter(r, r.Size())))
	}
	return
}

func (c *checkPoint) calFloor(off int64) int64 {
	remain := off & int64(c.blkSize-1)
	off -= remain
	if remain != 0 {
		off += int64(c.blkSize)
	}
	return off
}

func (c *checkPoint) Save(w io.Writer) error {
	buf := bytes.NewBuffer(make([]byte, 0, 512))
	buf.Write(MagicBytes)
	if err := gob.NewEncoder(buf).Encode(c.Data); err != nil {
		panic(err)
	}
	padding := c.blkSize - (buf.Len() & (c.blkSize - 1))
	buf.Write(c.blk[:padding])
	oldBlkOff := c.blkOff
	c.blkOff += int64(buf.Len())

	_, err := buf.WriteTo(w)
	if err != nil {
		c.blkOff = oldBlkOff
		err = logex.Trace(err)
	}
	return logex.Trace(err)
}

func (c *checkPoint) SetInoOffset(name string, offset int64) {
	c.Lock()
	c.Data[name] = offset
	c.Unlock()
}

// return -1 if ino not found
func (c *checkPoint) GetInoOffset(name string) int64 {
	c.Lock()
	off, ok := c.Data[name]
	c.Unlock()
	if !ok {
		return -1
	}
	return off
}
