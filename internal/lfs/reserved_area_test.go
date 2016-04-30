package lfs

import (
	"testing"
	"unsafe"

	"github.com/chzyer/test"
)

func TestIndirectInode(t *testing.T) {
	defer test.New(t)

	ra := NewReservedArea()
	test.Equal(unsafe.Sizeof(ra), ra.Size())
}

func TestInode(t *testing.T) {
	defer test.New(t)
	test.Equal(unsafe.Sizeof(Inode{}), new(Inode).Size())
}

func TestSuperblock(t *testing.T) {
	defer test.New(t)
	test.Equal(unsafe.Sizeof(Superblock{}), new(Superblock).Size())
}
