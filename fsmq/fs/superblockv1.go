package fs

import "unsafe"

const (
	IndirectAddrSize = ReserveBlockSize - MetaSize
)

type MetadataV1 struct {
	ByteOrder  int64 // 0: little endian, 1: big endian
	Version    int64
	BlockBit   int64
	CheckPoint int64
}

type SuperblockV1 struct {
	MetadataV1
	padding      [MetaSize - unsafe.Sizeof(MetadataV1{})]byte
	IndirectAddr [IndirectAddrSize / 8]int64
}

func (s *SuperblockV1) Size() int {
	return int(unsafe.Sizeof(*s))
}

func (s *SuperblockV1) version() int64 {
	return 1
}
