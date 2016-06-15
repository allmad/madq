package fs

const (
	BlockBit  = 18
	BlockSize = 1 << BlockBit
)

func MakeRoom(b []byte, n int) []byte {
	for {
		if n <= cap(b)-len(b) {
			return b[:len(b)+n]
		}
		b = append(b, 0)
	}
}
