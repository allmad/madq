package fs

import "encoding/binary"

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
