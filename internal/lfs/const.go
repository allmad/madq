package lfs

import (
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
)

const (
	BlockBit  = 12
	BlockSize = 1 << BlockBit
	Ones52    = 0xFFFFFFFFFFFFF
	Ones12    = 0xFFF
)

// ReservedArea
const (
	ReservedAreaSize = 128 * BlockSize
)

// -----------------------------------------------------------------------------
// InodeTable

const InodeTableSize = 128

var (
	InodeTableMagic        = []byte{0x8a, 0x9c, 0x0, 0x1}
	ErrDecodeNotInodeTable = logex.Define("not inode")
)

// -----------------------------------------------------------------------------
// Inode

const InodeSize = 128

var (
	InodeMagic        = []byte{0x8a, 0x9c, 0x0, 0x2}
	ErrDecodeNotInode = logex.Define("not indirect inode")
)

// -----------------------------------------------------------------------------

type Inode struct {
	Magic [4]byte
	Ino   int32

	Start  int64
	End    int64
	Create int64
	// the address of previous IndirectBlock/Inode/IndirectIndo
	Prev      int64
	BlockMeta [11]BlockMeta
}

func (n *Inode) ReadDisk(r bio.DiskReader) error {
	if r.Verify(InodeMagic) {
		return ErrDecodeNotInode.Trace()
	}
	n.Ino = r.Int32()

	n.Start = r.Int64()
	n.End = r.Int64()
	n.Create = r.Int64()
	n.Prev = r.Int64()
	for i := 0; i < len(n.BlockMeta); i++ {
		n.BlockMeta[i] = BlockMeta(r.Int64())
	}
	return nil
}

func (n *Inode) Size() int {
	return InodeSize
}

func (n *Inode) WriteDisk(w bio.DiskWriter) {
	w.Byte(InodeMagic)
	w.Int32(n.Ino)
	w.Int64(n.Start)
	w.Int64(n.End)
	w.Int64(n.Create)
	w.Int64(n.Prev)
	for i := 0; i < len(n.BlockMeta); i++ {
		w.Int64(int64(n.BlockMeta[i]))
	}
}

// -----------------------------------------------------------------------------

type BlockMeta int64

func (b BlockMeta) IsEmpty() bool {
	return b == 0
}

func (b *BlockMeta) SetPadding(n int16) {
	set := BlockMeta(n & 0xFFF) // 12 bit
	*b = *b & Ones52            // 52 bit
	*b = *b | (set << 52)
}

func (b *BlockMeta) SetAddr(n int64) {
	set := BlockMeta(n & Ones52)
	*b = (*b ^ Ones52) & set
}

func (b BlockMeta) GetLength() int {
	return BlockSize - int(b>>52)
}

func (b BlockMeta) GetAddr() int64 {
	return int64(b & Ones52)
}

// -----------------------------------------------------------------------------

const AddressSize = 8

type Address int64

func (i Address) Valid() bool {
	return i != 0
}

func (i *Address) Set(val Address) {
	*i = val
}

func (i Address) ReadAddr(raw bio.RawDisker, b []byte) (int, error) {
	return raw.ReadAt(b, int64(i))
}

func (i *Address) ReadDisk(r bio.DiskReader) error {
	*i = Address(r.Int64())
	return nil
}

func (i Address) Size() int {
	return AddressSize
}

func (i Address) WriteDisk(w bio.DiskWriter) {
	w.Int64(int64(i))
}
