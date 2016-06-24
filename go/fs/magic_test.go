package fs

import (
	"testing"

	"github.com/chzyer/test"
)

func TestMagic(t *testing.T) {
	defer test.New(t)
	test.Equal(MagicInode.String(), "Inode")
	test.Equal(Magic((&[4]byte{})[:]).String(), "unknown: 00000000")
}
