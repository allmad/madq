package lfs

import (
	"fmt"
	"unsafe"

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
	ErrDecodeNotInodeTable = logex.Define("not inodeTable")
)

// -----------------------------------------------------------------------------
// Inode

const InodeSize = 128

var (
	InodeMagic        = []byte{0x8a, 0x9c, 0x0, 0x2}
	ErrDecodeNotInode = logex.Define("not inode")
)

// -----------------------------------------------------------------------------

// 1G 要读1万个 ...
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

func (n *Inode) String() string {
	return fmt.Sprintf(
		"{Ino: %v, Start: %v, End: %v, Block: %v}",
		n.Ino, n.Start, n.End, n.BlockMeta,
	)
}

func (n *Inode) InitBlock(idx int, addr int64) {
	n.BlockMeta[idx].Set(0, addr)
}

func (n *Inode) SetBlock(idx int, size int, addr int64) {
	n.BlockMeta[idx].Set(size, addr)
	n.End = n.Start + int64(size)
}

func (n *Inode) AddBlockSize(idx int, size int) {
	n.BlockMeta[idx].AddLength(size)
	n.End += int64(size)
}

func (n *Inode) FindAvailable() int {
	for idx, b := range n.BlockMeta {
		if b.IsEmpty() {
			return idx
		}
	}
	return -1
}

func (n *Inode) ReadDisk(r bio.DiskReader) error {
	if !r.Verify(InodeMagic) {
		r.Skip(-len(InodeMagic))
		return ErrDecodeNotInode.Trace(r.Byte(len(InodeMagic)))
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

func (b BlockMeta) String() string {
	if b.IsEmpty() {
		return "nil"
	}
	return fmt.Sprintf("{addr: %v, len: %v}", b.GetAddr(), b.GetLength())
}

func (b BlockMeta) IsEmpty() bool {
	return b == 0
}

func (b *BlockMeta) Set(length int, addr int64) {
	*b = BlockMeta(int64(((BlockSize-length)&0XFFF)<<52) + addr&Ones52)
}

func (b *BlockMeta) AddLength(n int) {
	b.SetLength(b.GetLength() + n)
}

func (b *BlockMeta) SetLength(n int) {
	b.SetPadding(BlockSize - int16(n))
}

func (b *BlockMeta) SetPadding(n int16) {
	if n < 0 {
		panic("set negative padding")
	}
	set := BlockMeta(n & 0xFFF) // 12 bit
	*b = *b & Ones52            // 52 bit
	*b = *b | (set << 52)
}

func (b BlockMeta) GetLength() int {
	return BlockSize - (int(b>>52) & 0XFFF)
}

func (b BlockMeta) GetAddr() int64 {
	return int64(b & Ones52)
}

// -----------------------------------------------------------------------------

const AddressSize = 8

type Address int64

func InodeAddress(i *Inode) Address {
	return FakeAddress(uintptr(unsafe.Pointer(i)))
}

func InodeTableAddress(i *InodeTable) Address {
	return FakeAddress(uintptr(unsafe.Pointer(i)))
}

func FakeAddress(ptr uintptr) Address {
	return Address(-int64(ptr))
}

func (i Address) InMemory() bool {
	return i < 0
}

func (i Address) Valid() bool {
	return i != 0
}

func (i *Address) Update(val Address) {
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
