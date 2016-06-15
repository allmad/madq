package fs

import "github.com/chzyer/logex"

var _ Diskable = new(Inode)

const InodePadding = 44

// size: 1kB
// one inode can store 37.5MB
type Inode struct {
	// Magic 4
	Ino   Int32
	Start Int32
	Size  Int32 // 8

	Prev,
	Prev2,
	Prev4,
	Prev8,
	Prev16,
	Prev32,
	PrevGroup Address // 7*8

	GroupSize Int32
	GroupIdx  Int32 // 8

	// padding : 1024 - (150*6) - 76 - Magic(4) = 44

	Offsets [150]ShortAddr

	addr Address // my addr in disk/mem
}

func (i *Inode) IsFull() bool {
	return int(i.Size) == BlockSize*len(i.Offsets)
}

func (i *Inode) GetBlockSize(idx int) int {
	lastIdx := i.GetOffsetIdx()
	if idx == lastIdx {
		return i.Size % BlockSize
	}
	return BlockSize
}

func (i *Inode) GetOffsetIdx() int {
	// 256k per Offset
	return int(i.Size) >> BlockBit
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

	dw.WriteItem(i.Prev)
	dw.WriteItem(i.Prev2)
	dw.WriteItem(i.Prev4)
	dw.WriteItem(i.Prev8)
	dw.WriteItem(i.Prev16)
	dw.WriteItem(i.Prev32)
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
		&i.Prev, &i.Prev2, &i.Prev4, &i.Prev8,
		&i.Prev16, &i.Prev32, &i.PrevGroup,

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
