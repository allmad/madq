package fs

import (
	"bytes"
	"fmt"
)

type Magic []byte

const MagicSize = 4

func (m Magic) String() string {
	items := []struct {
		m    Magic
		name string
	}{
		{MagicEOF, "EOF"},
		{MagicVolume, "Volume"},
		{MagicInode, "Inode"},
	}

	for _, i := range items {
		if bytes.Equal(i.m, m) {
			return i.name
		}
	}

	return fmt.Sprintf("unknown: %x", []byte(m))
}

var (
	MagicEOF    = Magic{0x8a, 0x9b, 0x0, 0x1}
	MagicVolume = Magic{0x8a, 0x9b, 0x0, 0x2}
	MagicInode  = Magic{0x8a, 0x9b, 0x0, 0x3}
)
