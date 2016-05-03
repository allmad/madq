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

func NewReservedArea() *ReservedArea {
	ra := &ReservedArea{}
	ra.Superblock.Init()
	return ra
}

func (r *ReservedArea) GetInoStartByIndirInodeTbl(i int) int {
	return i * 15
}

// get the index of IndirectInodeTable and InodeTable
func (r *ReservedArea) GetIdx(ino int32) (idxL1, idxL2 int) {
	idxL1 = int(ino / 15) // 15 => number of Inode in InodeTable
	idxL2 = int(ino % 15)
	return
}

func (r *ReservedArea) ReadDisk(reader bio.DiskReader) error {
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

func (r *ReservedArea) WriteDisk(w bio.DiskWriter) {
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
	padding    [BlockSize - 16]byte
	Checkpoint int64
}

func (s *Superblock) Init() {
	s.Version = 1
	s.InodeCnt = 1 // Root Inode
}

func (*Superblock) Size() int {
	return SuperblockSize
}

func (s *Superblock) ReadDisk(r bio.DiskReader) error {
	s.Version = r.Int32()
	s.InodeCnt = r.Int32()
	s.Checkpoint = r.Int64()
	r.Skip(len(s.padding))
	return nil
}

func (s *Superblock) WriteDisk(w bio.DiskWriter) {
	w.Int32(s.Version)
	w.Int32(s.InodeCnt)
	w.Int64(s.Checkpoint)
	w.Skip(len(s.padding))
}

// -----------------------------------------------------------------------------

type InodeTable struct {
	Magic [4]byte
	_     int32

	Address [15]Address
}

func (i *InodeTable) FindAvailable() int {
	for idx, addr := range i.Address {
		if !addr.Valid() {
			return idx
		}
	}
	return -1
}

func (i *InodeTable) Size() int {
	return InodeTableSize
}

func (i *InodeTable) ReadDisk(r bio.DiskReader) error {
	if !r.Verify(InodeTableMagic) {
		r.Skip(-len(InodeTableMagic))
		return ErrDecodeNotInodeTable.Trace(r.Byte(len(InodeTableMagic)))
	}
	r.Skip(4)

	for idx := range i.Address {
		_ = i.Address[idx].ReadDisk(r)
	}
	return nil
}

func (i *InodeTable) WriteDisk(w bio.DiskWriter) {
	w.Byte(InodeTableMagic)
	w.Skip(4)
	for _, addr := range i.Address {
		addr.WriteDisk(w)
	}
}
