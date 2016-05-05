package lfs

import (
	"testing"
	"unsafe"

	"github.com/chzyer/test"
)

func TestReservedArea(t *testing.T) {
	defer test.New(t)

	ra := NewReservedArea()
	test.Equal(unsafe.Sizeof(*ra), ra.Size())

	{ // check get idx
		mm := [MaxInodeSize / InodeTableCap][InodeTableCap]bool{}
		for i := 0; i < MaxInodeSize; i++ {
			l1, l2 := ra.GetIdx(int32(i))
			mm[l1][l2] = true
		}
		for idxL1, mmm := range mm {
			for idxL2, mmmm := range mmm {
				if !mmmm {
					t.Fatal(idxL1, idxL2)
				}
			}
		}
	}
}

func TestInode(t *testing.T) {
	defer test.New(t)
	test.Equal(unsafe.Sizeof(Inode{}), new(Inode).Size())
}

func TestSuperblock(t *testing.T) {
	defer test.New(t)
	test.Equal(unsafe.Sizeof(Superblock{}), new(Superblock).Size())
}
