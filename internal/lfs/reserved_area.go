package lfs

import (
	"bytes"

	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
)

// 1 block
type Superblock struct {
	Version    int32
	Checkpoint int64
	padding    [BlockSize - 8 - 4]byte
}

func (*Superblock) Size() int {
	return SuperblockSize
}

func (s *Superblock) ReadDisk(r *bio.Reader) error {
	s.Version = r.Int32()
	s.Checkpoint = r.Int64()
	return nil
}

func (s *Superblock) WriteDisk(w *bio.Writer) {
	w.Int32(s.Version)
	w.Int64(s.Checkpoint)
}

// TODO(chzyer): add the size on Tables in testcase
type ReservedArea struct {
	Superblock         Superblock
	InodeTable         [47 * 32]Inode
	IndirectInodeTable [16 * 512]IndirectInodeAddr
}

func NewReservedArea() ReservedArea {
	return ReservedArea{}
}

func (r *ReservedArea) ReadDisk(reader *bio.Reader) error {
	if err := reader.ReadDisk(&r.Superblock); err != nil {
		return logex.Trace(err)
	}

	for i := 0; i < len(r.InodeTable); i++ {
		if err := r.InodeTable[i].ReadDisk(reader); err != nil {
			return logex.Trace(err)
		}
	}

	for i := 0; i < len(r.IndirectInodeTable); i++ {
		if err := r.IndirectInodeTable[i].ReadDisk(reader); err != nil {
			return logex.Trace(err)
		}
	}
	return nil
}

func (*ReservedArea) Size() int {
	return ReservedAreaSize
}

func (r *ReservedArea) WriteDisk(w *bio.Writer) {
	r.Superblock.WriteDisk(w)
	for i := 0; i < len(r.InodeTable); i++ {
		r.InodeTable[i].WriteDisk(w)
	}
	for i := 0; i < len(r.IndirectInodeTable); i++ {
		r.IndirectInodeTable[i].WriteDisk(w)
	}
}

// -----------------------------------------------------------------------------

const InodeSize = 128

type Inode struct {
	Create        int64
	End           int64
	IndirectBlock int64
	BlockMeta     [13]BlockMeta
}

func (*Inode) Size() int {
	return InodeSize
}

func (n *Inode) ReadDisk(r *bio.Reader) error {
	n.Create = r.Int64()
	n.End = r.Int64()
	n.IndirectBlock = r.Int64()
	for i := 0; i < len(n.BlockMeta); i++ {
		n.BlockMeta[i] = BlockMeta(r.Int64())
	}
	return nil
}

func (n *Inode) WriteDisk(w *bio.Writer) {
	w.Int64(n.Create)
	w.Int64(n.End)
	w.Int64(n.IndirectBlock)
	for i := 0; i < len(n.BlockMeta); i++ {
		w.Int64(int64(n.BlockMeta[i]))
	}
}

// -----------------------------------------------------------------------------

const IndirectInodeSize = 128

var (
	IndirectInodeMagic  = []byte{0x8a, 0x9c, 0x0, 0x1}
	ErrNotIndirectInode = logex.Define("not indirect inode")
)

const IndirectInodeTableSize = 8

type IndirectInodeAddr int64

func (i *IndirectInodeAddr) ReadDisk(r *bio.Reader) error {
	*i = IndirectInodeAddr(r.Int64())
	return nil
}

func (i IndirectInodeAddr) Size() int {
	return 8
}

func (i IndirectInodeAddr) WriteDisk(w *bio.Writer) {
	w.Int64(int64(i))
}

type IndirectInode struct {
	Magic         [4]byte
	End           int32
	Create        int64
	IndirectBlock int64
	BlockMeta     [13]BlockMeta
}

func (n *IndirectInode) ReadDisk(r *bio.Reader) error {
	if bytes.Equal(r.Byte(4), IndirectInodeMagic) {
		return ErrNotIndirectInode.Trace()
	}
	n.End = r.Int32()
	n.Create = r.Int64()
	n.IndirectBlock = r.Int64()
	for i := 0; i < len(n.BlockMeta); i++ {
		n.BlockMeta[i] = BlockMeta(r.Int64())
	}
	return nil
}

func (n *IndirectBlock) Size() int {
	return IndirectInodeSize
}

func (n *IndirectInode) WriteDisk(w *bio.Writer) {
	w.Byte(IndirectInodeMagic)
	w.Int32(n.End)
	w.Int64(n.Create)
	w.Int64(n.IndirectBlock)
	for i := 0; i < len(n.BlockMeta); i++ {
		w.Int64(int64(n.BlockMeta[i]))
	}
}
