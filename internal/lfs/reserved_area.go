package lfs

import (
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
)

const (
	InodeTableCap = 15
	MaxInodeSize  = 127 * 512 * InodeTableCap
)

// TODO(chzyer): add the size on Tables in testcase
type ReservedArea struct {
	Superblock         Superblock
	IndirectInodeTable [MaxInodeSize / InodeTableCap]Address
}

func NewReservedArea() ReservedArea {
	ra := ReservedArea{}
	ra.Superblock.Version = 1
	return ra
}

// get the index of IndirectInodeTable and InodeTable
func (r *ReservedArea) GetIdx(ino int) (idxL1, idxL2 int) {
	idxL1 = ino / 15 // 15 => number of Inode in InodeTable
	idxL2 = ino % 15
	return
}

func (r *ReservedArea) ReadDisk(reader *bio.Reader) error {
	if err := reader.ReadDisk(&r.Superblock); err != nil {
		return logex.Trace(err)
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
	for i := 0; i < len(r.IndirectInodeTable); i++ {
		r.IndirectInodeTable[i].WriteDisk(w)
	}
}

// -----------------------------------------------------------------------------

const SuperblockSize = BlockSize

// 1 block
type Superblock struct {
	Version    int32
	InodeCnt   int32
	padding    [BlockSize - 12]byte
	Checkpoint int64
}

func (*Superblock) Size() int {
	return SuperblockSize
}

func (s *Superblock) ReadDisk(r *bio.Reader) error {
	s.Version = r.Int32()
	s.InodeCnt = r.Int32()
	s.Checkpoint = r.Int64()
	return nil
}

func (s *Superblock) WriteDisk(w *bio.Writer) {
	w.Int32(s.Version)
	w.Int32(s.InodeCnt)
	w.Int64(s.Checkpoint)
	w.Padding(len(s.padding))
}

// -----------------------------------------------------------------------------

type InodeTable struct {
	Magic [4]byte
	_     int32

	Address [15]Address
}

func (i *InodeTable) Size() int {
	return InodeTableSize
}

func (i *InodeTable) ReadDisk(r *bio.Reader) error {
	if !r.Verify(InodeTableMagic) {
		return ErrDecodeNotInodeTable
	}
	r.Skip(4)

	for _, addr := range i.Address {
		_ = addr.ReadDisk(r)
	}
	return nil
}

func (i *InodeTable) WriteDisk(w *bio.Writer) {
	w.Byte(InodeTableMagic)
	w.Padding(4)
	for _, addr := range i.Address {
		addr.WriteDisk(w)
	}
}