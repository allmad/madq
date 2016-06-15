package fs

import (
	"bytes"
	"fmt"
)

type Magic []byte

func (m Magic) String() string {
	items := []struct {
		m    Magic
		name string
	}{
		{MagicInode, "inode"},
	}

	for _, i := range items {
		if bytes.Equal(i.m, m) {
			return i.name
		}
	}

	return fmt.Sprintf("unknown: %x", m)
}

var (
	MagicEOF   = Magic{0x8a, 0x9c, 0x0, 0x1}
	MagicInode = Magic{0x8a, 0x9c, 0x0, 0x2}
)
