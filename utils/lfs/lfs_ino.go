package lfs

import (
	"container/list"
	"io"

	"github.com/chzyer/muxque/rpc"
	"github.com/chzyer/muxque/utils"
	"gopkg.in/logex.v1"
)

const (
	InoMaxNameSize = 64
	InoBlkSize     = 256
)

var (
	ErrFileNameTooLarge = logex.Define("filename is too long")
)

type InoIndirect struct {
	Start *Inode
	Size  int

	tbl *list.List
}

func blkInfoSet(remain int, off int64) int64 {
	return (int64(remain&InoSizeMax) << InoOffsetMax) | (off & InoOffsetMax)
}

func blkInfoGet(info int64) (remain int, off int64) {
	return int(info>>InoOffsetMax) & InoSizeMax, info & InoOffsetBit
}

type inodeShare struct {
	Name     *rpc.String
	FileSize int64

	tbl     *list.List
	blkBit  uint
	blkSize int
}

// The size of Inode Must less than one block(4KB)
// Name: 64
// FileSize: 8
// StartOff: 8
// Blks: 256 => 1MB
// Blk => remain(16) | off(48) => int64
type Inode struct {
	*inodeShare

	IndirIno int64 // offset of previous inode
	PrevSize int64 // the size before this inode
	Blks     [InoBlkSize]int64

	blkOff int
	elem   *list.Element
}

func ReadInode(r io.Reader, bit uint) (*Inode, error) {
	return ReadInodeEx(r, &inodeShare{
		blkBit:  bit,
		blkSize: 1 << bit,
	})
}

func ReadInodeEx(r io.Reader, share *inodeShare) (*Inode, error) {
	return nil, nil
}

func NewInode(name *rpc.String, bit uint) (*Inode, error) {
	if name.PSize() > InoMaxNameSize {
		return nil, ErrFileNameTooLarge.Trace()
	}

	ino := &Inode{
		inodeShare: &inodeShare{
			Name: name,

			tbl:     list.New(),
			blkBit:  bit,
			blkSize: 1 << bit,
		},
	}
	ino.elem = ino.tbl.PushFront(ino)
	return ino, nil
}

func (ino *Inode) getBlkSize(info int64) (size int, offset int64) {
	remain, offset := blkInfoGet(info)
	return ino.blkSize - remain, offset
}

func (ino *Inode) Indirect(indirino int64) *Inode {
	ino2 := &Inode{
		inodeShare: ino.inodeShare,
		IndirIno:   indirino,
	}
	ino2.elem = ino.tbl.InsertBefore(ino2, ino.elem)
	return ino2
}

func (ino *Inode) prev() *Inode {
	elem := ino.elem.Prev()
	if elem == nil {
		return nil
	}
	return elem.Value.(*Inode)
}

func (ino *Inode) prevEx(ra io.ReaderAt) *Inode {
	elem := ino.elem.Prev()
	if elem != nil {
		return elem.Value.(*Inode)
	}

	if ino.IndirIno == 0 {
		return nil
	}

	// read from disk
	r := utils.NewReader(ra, ino.IndirIno)
	preIno, err := ReadInodeEx(r, ino.inodeShare)
	if err != nil {
		return nil
	}
	preIno.elem = ino.tbl.PushBack(preIno)
	return preIno
}

func (ino *Inode) next() *Inode {
	elem := ino.elem.Next()
	if elem == nil {
		return nil
	}
	return elem.Value.(*Inode)
}

// seek target inode from specified offset one by one
func (ino *Inode) seekIno(off int64, r io.ReaderAt) *Inode {
	var another *Inode
	if off > ino.PrevSize {
		for {
			another = ino.next()
			if another != nil && off > another.PrevSize {
				ino = ino.next()
				continue
			}
			break
		}
	} else {
		for {
			another = ino.prevEx(r)
			if another != nil && another.PrevSize > off {
				ino = ino.prevEx(r)
				continue
			}
			if ino.PrevSize > off {
				// inode may truncate
				return nil
			}
			break
		}
	}
	return ino
}

func (ino *Inode) findRawOff(off int64) (blkidx int, rawOff int64) {
	offset := ino.PrevSize
	for i := 0; i < ino.blkOff; i++ {
		size, rawOff := ino.getBlkSize(ino.Blks[i])
		if off < offset+int64(size) {
			return i, rawOff + (off - offset)
		}
		offset += int64(size)
	}
	return -1, -1
}

func (ino *Inode) GetRawOff(off int64, r io.ReaderAt) (rawOff int64, newIno *Inode, blkIdx int) {
	ino = ino.seekIno(off, r)
	if ino == nil {
		return -1, nil, -1
	}

	// search
	blkIdx, rawOff = ino.findRawOff(off)
	return rawOff, ino, blkIdx
}

type OffsetWriter interface {
	io.Writer
	GetOff() int64
}

func (ino *Inode) FullExtBlk(ow OffsetWriter, rawOff int64, size int) (newRawOff int64, newSize int, err error) {
	var offset int64
	for size > 0 {
		rawOff, size = ino.ExtBlks(rawOff, size)
		if size > 0 {
			// flush, get offset
			offset = ow.GetOff()
			err = ino.PWrite(ow)
			if err != nil {
				return rawOff, size, logex.Trace(err)
			}

			newIno := ino.Indirect(offset)
			*ino = *newIno
		}
	}
	return
}

func (ino *Inode) ExtBlks(rawOff int64, size int) (int64, int) {
	blkcnt := size >> ino.blkBit
	destBlk := ino.blkOff + blkcnt
	if destBlk > InoBlkSize {
		destBlk = InoBlkSize
	}
	remain := 0
	off := rawOff
	for ; ino.blkOff < destBlk; ino.blkOff++ {
		if remain == destBlk-1 {
			remain = size & (ino.blkSize - 1)
		}
		ino.Blks[ino.blkOff] = blkInfoSet(remain, rawOff)
		rawOff += int64(ino.blkSize)
		size -= ino.blkSize - remain
	}

	return rawOff, size
}

func (ino *Inode) PWrite(w io.Writer) error {
	return nil
}

func (ino *Inode) PRead(r io.Reader) error {
	return nil
}
