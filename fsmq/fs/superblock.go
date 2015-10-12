package fs

import (
	"bufio"
	"io"
	"reflect"
	"unsafe"

	"github.com/chzyer/fsmq/fsmq/util/qio"
	"github.com/klauspost/crc32"
	"gopkg.in/logex.v1"
)

const (
	BlockBit         = 12
	MetaSize         = 1 << 10
	ReserveBlocks    = 64
	BlockSize        = 1 << BlockBit
	ReserveBlockSize = ReserveBlocks * BlockSize
)

var (
	ErrByteOrderNotEqual = logex.Define("current byte order is not equal to Volume does, current byte order: %v")
	ErrSbInvalidVersion  = logex.Define("superblock: invalid version")
	ErrSbInvalidLen      = logex.Define("superblock: invalid data size")
	ErrSbInvalid         = logex.Define("superblock: invalid superblock")

	ErrNotFound = logex.Define("superblock: inode not found")
)

type InodeName [24]byte

var EmptyInodeName InodeName

func (i *InodeName) IsEmpty() bool {
	return *i == EmptyInodeName
}

type IndirectSuperblock [BlockSize / unsafe.Sizeof(InodeIndex{})]InodeIndex

type InodeIndex struct {
	Name    InodeName
	Address int64
}

type Superblock struct {
	*SuperblockV1
	ref *[]byte // hold on ref

	inodeAddr map[InodeName]int64
}

func NewSuperblock(w io.WriterAt, blockBit int64) *Superblock {
	s := new(Superblock)
	s.init(w)
	return s
}

func NewSuperblockByBytes(w io.WriterAt, b []byte) (s *Superblock, err error) {
	err = s.Decode(b)
	s.init(w)
	return
}

func (s *Superblock) init(w io.WriterAt) {
	if s == nil {
		return
	}
	s.inodeAddr = make(map[InodeName]int64)
	go s.ioloop(w)
}

func (s *Superblock) ioloop(w io.WriterAt) {
}

func (s *Superblock) AddInodeAddr(name InodeName, addr int64) {
	s.inodeAddr[name] = addr
}

func (s *Superblock) findByName(name InodeName, r io.ReaderAt) (int64, error) {
	addr, ok := s.inodeAddr[name]
	if ok {
		return addr, nil
	}
	hash := crc32.ChecksumIEEE(name[:])
	idx := int(hash) % len(s.IndirectAddr)
	for tryTime := 0; tryTime < 8; tryTime++ {
		offset, err := s.findAtBlock(name, r, s.IndirectAddr[idx])
		if err == nil {
			return offset, nil
		}
		if logex.Equal(err, io.EOF) {
			idx++
			continue
		}
		return 0, logex.Trace(err)
	}
	return 0, ErrNotFound.Trace()
}

func (s *Superblock) findAtBlock(name InodeName, ra io.ReaderAt, off int64) (int64, error) {
	var r io.Reader = qio.NewReader(ra, off)
	r = bufio.NewReader(io.LimitReader(r, BlockSize))
	var buf [unsafe.Sizeof(InodeIndex{})]byte
	var index *InodeIndex
	for {
		n, err := r.Read(buf[:])
		if err != nil {
			return 0, err
		}
		if n != len(buf) {
			return 0, ErrNotFound.Trace()
		}
		index = (*InodeIndex)(unsafe.Pointer(&buf))
		if index.Name.IsEmpty() {
			return 0, ErrNotFound.Trace()
		}
		s.inodeAddr[index.Name] = index.Address
	}

	if addr, ok := s.inodeAddr[name]; ok {
		return addr, nil
	}
	return 0, ErrNotFound.Trace()
}

func (s *Superblock) Size() int {
	return int(unsafe.Sizeof(*s.SuperblockV1))
}

func (s *Superblock) GetByteOrder() ByteOrder {
	num := 1
	b := (*(*byte)(unsafe.Pointer(&num)))
	bo := BigEndian
	if b == 1 {
		bo = LittleEndian
	}
	return bo
}

func (s *Superblock) Encode() []byte {
	s.ByteOrder = int64(s.GetByteOrder())
	s.Version = s.version()
	addr := (*[ReserveBlockSize]byte)(unsafe.Pointer(s.SuperblockV1))
	return (*addr)[:]
}

func (s *Superblock) Decode(b []byte) error {
	if len(b) < 16 {
		return ErrSbInvalidLen.Trace(len(b))
	}
	// check byteOrder
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteOrder := ByteOrder(*(*int64)(unsafe.Pointer(sh.Data)))
	if byteOrder != s.GetByteOrder() {
		// TODO: support auto convert when ByteOrder not equals to current
		return ErrByteOrderNotEqual.Format(s.GetByteOrder())
	}

	// check version
	version := *(*int64)(unsafe.Pointer(sh.Data + 8))
	switch version {
	case 1:
		if s.SuperblockV1.Size() != len(b) {
			return ErrSbInvalid.Trace(len(b))
		}
		buf := make([]byte, len(b))
		copy(buf, b)
		s.ref = &buf
		sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
		s.SuperblockV1 = (*SuperblockV1)(unsafe.Pointer(sh.Data))
	default:
		return ErrSbInvalidVersion.Trace(version)
	}

	return nil
}
