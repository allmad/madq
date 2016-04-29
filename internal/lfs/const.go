package lfs

const (
	BlockBit  = 12
	BlockSize = 1 << BlockBit
	Ones52    = 0xFFFFFFFFFFFFF
	Ones12    = 0xFFF
)

// ReservedArea
const (
	ReservedAreaSize        = 64 * BlockSize
	SuperblockSize          = BlockSize
	InodeTableStart         = BlockSize
	InodeTableEnd           = InodeTableStart + 47*BlockSize
	IndirectInodeTableStart = InodeTableEnd
	IndirectInodeTableEnd   = InodeTableEnd + 16*BlockSize
)
