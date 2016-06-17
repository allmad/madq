package fs

import "github.com/chzyer/logex"

var _ Diskable = new(Inode)
var emptyPrevs = [6]*Address{
	new(Address), new(Address), new(Address), new(Address), new(Address), new(Address),
}

const InodePadding = 44
const InodeSize = 1024
const InodeBlockCnt = 150
const InodeCap = InodeBlockCnt * BlockSize

// size: 1kB
// one inode can store 37.5MB
type Inode struct {
	// Magic 4
	Ino   Int32
	Start Int32
	Size  Int32 // 8

	// 1, 2, 4, 8, 16, 32
	PrevInode [6]*Address
	PrevGroup Address // 7*8

	GroupSize Int32
	GroupIdx  Int32 // 8

	// padding : 1024 - (150*6) - 76 - Magic(4) = 44

	Offsets [InodeBlockCnt]ShortAddr

	addr Address // my addr in disk/mem
}

func NewInode(ino int32) *Inode {
	return &Inode{
		Ino:       Int32(ino),
		PrevInode: emptyPrevs,
	}
}

func (i *Inode) IsFull() bool {
	return int(i.Size) == BlockSize*len(i.Offsets)
}

// off: global offset
// off must in this Inode
func (i *Inode) GetRemainInBlock(off int64) int {
	// off in block
	idx := i.GetBlockIdx(off)
	offBlk := int(off & (BlockSize - 1))
	remain := i.GetBlockSize(idx) - offBlk
	if remain < 0 {
		panic("off not in inode")
	}
	return remain
}

func (i *Inode) GetBlockIdx(off int64) int {
	off -= int64(i.Start) * BlockSize
	if off > int64(i.Size) {
		panic("off not in inode")
	}
	return int(off) >> BlockBit
}

func (i *Inode) GetBlockSize(idx int) int {
	lastIdx := i.GetSizeIdx()
	if idx == lastIdx {
		return int(i.Size % BlockSize)
	}
	return BlockSize
}

// func (i *Inode) GetOffsetIdx()

func (i *Inode) GetSizeIdx() int {
	// 256k per Offset
	return int(i.Size) >> BlockBit
}

func (i *Inode) SetOffset(idx int, addr ShortAddr, size int) {
	i.Offsets[idx] = addr
	i.Size += Int32(size)
}

func (i *Inode) DiskSize() int { return 1024 }

func (i *Inode) Magic() Magic {
	return MagicInode
}

func (i *Inode) WriteDisk(b []byte) {
	dw := NewDiskWriter(b)
	dw.WriteMagic(i)

	dw.WriteItem(i.Ino)
	dw.WriteItem(i.Start)
	dw.WriteItem(i.Size)

	for _, p := range i.PrevInode {
		dw.WriteItem(p)
	}
	dw.WriteItem(i.PrevGroup)

	dw.WriteItem(i.GroupSize)
	dw.WriteItem(i.GroupIdx)

	// padding
	dw.Skip(InodePadding)

	for k := 0; k < len(i.Offsets); k++ {
		if i.Offsets[k] == 0 {
			break
		}
		dw.WriteItem(i.Offsets[k])
	}
}

func (i *Inode) ReadDisk(b []byte) error {
	dr := NewDiskReader(b)

	if err := dr.ReadMagic(i); err != nil {
		return logex.Trace(err)
	}

	if err := dr.ReadItems([]DiskReadItem{
		&i.Ino, &i.Start, &i.Size,
		i.PrevInode[0], i.PrevInode[1], i.PrevInode[2], i.PrevInode[3],
		i.PrevInode[4], i.PrevInode[5], &i.PrevGroup,

		&i.GroupSize, &i.GroupIdx,
	}); err != nil {
		return logex.Trace(err)
	}

	dr.Skip(InodePadding)

	for k := 0; k < len(i.Offsets); k++ {
		if err := dr.ReadItem(&i.Offsets[k]); err != nil {
			return logex.Trace(err)
		}
		if i.Offsets[k] == 0 {
			break
		}
	}

	return nil
}

func (i *Inode) SeekIdx(offset int64) (int, bool) {
	if offset > int64(i.Start*BlockSize+InodeCap) {
		return -1, false
	}

	return int((offset & InodeCap) >> BlockBit), true
}
