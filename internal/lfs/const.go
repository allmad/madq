package lfs

const (
	BlockBit     = 12
	BlockSize    = 1 << BlockBit
	ReservedAres = 64 * BlockSize
	Ones52       = 0xFFFFFFFFFFFFF
	Ones12       = 0xFFF
)
