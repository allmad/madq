package fs

import (
	"encoding/binary"
	"reflect"
	"unsafe"

	"gopkg.in/logex.v1"
)

const (
	IndexOff  = 1024
	BlockSize = 1 << 16
)

var (
	ErrByteOrderNotEqual = logex.Define("current byte order is not equal to Volume does, current byte order: %v")
	ErrSbInvalidVersion  = logex.Define("superblock: invalid version")
	ErrSbInvalidLen      = logex.Define("superblock: invalid data size")
	ErrSbInvalid         = logex.Define("superblock: invalid superblock")
)

type ByteOrder int64

const (
	LittleEndian ByteOrder = iota
	BigEndian
)

func (b ByteOrder) Binary() (s binary.ByteOrder) {
	s = binary.LittleEndian
	if b == BigEndian {
		s = binary.BigEndian
	}
	return s
}

func (b ByteOrder) String() string {
	switch b {
	case LittleEndian:
		return "littleEndian"
	case BigEndian:
		return "bidEndian"
	}
	return ""
}

// each block has 128 Entry
type SuperIndex struct {
	Key      int64
	Indirect int64
}

type SuperblockV1 struct {
	Metadata struct {
		ByteOrder  int64 // 0: little endian, 1: big endian
		Version    int64
		BlockBit   int64
		CheckPoint int64
	}
	SuperIndex [BlockSize]SuperIndex
}

func (s *SuperblockV1) Size() int {
	return int(unsafe.Sizeof(*s))
}

type Superblock struct {
	*SuperblockV1
	ref *[]byte // hold on ref
}

func (s *Superblock) Size() int {
	return int(unsafe.Sizeof(*s.SuperblockV1))
}

func (s *Superblock) ByteOrder() ByteOrder {
	num := 1
	b := (*(*byte)(unsafe.Pointer(&num)))
	bo := BigEndian
	if b == 1 {
		bo = LittleEndian
	}
	return bo
}

func (s *Superblock) Encode() []byte {
	s.Metadata.ByteOrder = int64(s.ByteOrder())
	addr := uintptr(unsafe.Pointer(s.SuperblockV1))
	size := s.Size()
	sh := &reflect.SliceHeader{
		Data: addr,
		Len:  size,
		Cap:  size,
	}
	return *(*[]byte)(unsafe.Pointer(sh))
}

func (s *Superblock) Decode(b []byte) error {
	if len(b) < 16 {
		return ErrSbInvalidLen.Trace(len(b))
	}
	// check byteOrder
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteOrder := ByteOrder(*(*int64)(unsafe.Pointer(sh.Data)))
	if byteOrder != s.ByteOrder() {
		// TODO: support auto convert when ByteOrder not equals to current
		return ErrByteOrderNotEqual.Format(s.ByteOrder())
	}

	// check version
	version := *(*int64)(unsafe.Pointer(sh.Data + 8))
	switch version {
	case 1:
		if s.SuperblockV1.Size() != len(b) {
			return ErrSbInvalid.Trace(len(b))
		}
		s.SuperblockV1 = (*SuperblockV1)(unsafe.Pointer(sh.Data))
	default:
		return ErrSbInvalidVersion.Trace(version)
	}

	// TODO
	s.ref = &b
	return nil
}
