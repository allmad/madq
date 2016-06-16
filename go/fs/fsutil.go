package fs

const (
	BlockBit  = 18
	BlockSize = 1 << BlockBit
)

func MakeRoom(b []byte, n int) []byte {
	for {
		if n <= cap(b)-len(b) {
			return b[:n]
		}
		b = append(b, 0)
	}
}

func GetBlockCnt(n int) int {
	ret := n >> BlockBit
	if n&(BlockSize-1) == 0 {
		return ret
	}
	return ret + 1
}

func FloorBlk(n int) int {
	if n&(BlockSize-1) == 0 {
		return n
	}
	return ((n >> BlockBit) + 1) << BlockBit
}
