package lfs

type BlockMeta int64

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

type IndirectBlock struct {
	Magic [4]byte
	Ino   int32
	Start int64
	End   int64
	// the address of previous IndirectBlock/Inode/IndirectIndo
	Prev      int64
	BlockMeta [12]BlockMeta
}
