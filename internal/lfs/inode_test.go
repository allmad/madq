package lfs

import (
	"testing"
	"unsafe"

	"github.com/chzyer/test"
)

func TestSize(t *testing.T) {
	defer test.New(t)
	test.Equal(unsafe.Sizeof(IndirectInode{}), 128)
	test.Equal(unsafe.Sizeof(Inode{}), InodeSize)
	test.Equal(unsafe.Sizeof(Superblock{}), BlockSize)
}
