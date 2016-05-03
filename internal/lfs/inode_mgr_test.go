package lfs

import (
	"sync/atomic"
	"testing"

	"github.com/chzyer/madq/internal/bio"
	"github.com/chzyer/test"
)

type inodeMgrDumpDelegate struct {
	pointer int64
}

func (i *inodeMgrDumpDelegate) Malloc(n int) int64 {
	return atomic.AddInt64(&i.pointer, int64(n)) - int64(n)
}

func TestInodeMgr(t *testing.T) {
	defer test.New(t)

	test.CleanTmp()
	block, err := bio.NewFile(test.Root())
	test.Nil(err)

	offset := int64(256)
	delegate := &inodeMgrDumpDelegate{offset}
	im := NewInodeMgr(delegate)

	{
		err := im.Init(block)
		test.Nil(err)
	}
	ptr := im.GetPointer()

	im.Start(bio.NewDevice(block, delegate.pointer, func(d *bio.Device) {
		atomic.StoreInt64(ptr, d.Offset())
	}))

	{
		inode, err := im.newInode()
		test.Nil(err)
		test.Equal(inode.Ino, 1)
		inode2, err := im.GetInode(inode.Ino)
		test.Nil(err)
		test.Equal(inode, inode2)
		err = im.deleteInode(inode.Ino)
		test.Nil(err)
		inode3, err := im.GetInode(inode.Ino)
		test.Nil(inode3)
		test.Equal(err, ErrInodeMgrInodeNotFound)
	}

	{
		inode, err := im.newInode()
		test.Nil(err)
		test.Equal(inode.Ino, 1)
		test.Nil(im.Flush())
		im := NewInodeMgr(delegate)
		{
			err := im.Init(block)
			test.Nil(err)
			im.Start(bio.NewDevice(block, atomic.LoadInt64(ptr), func(d *bio.Device) {
				atomic.StoreInt64(im.GetPointer(), d.Offset())
			}))
		}
		inode2, err := im.GetInode(inode.Ino)
		test.Nil(err)
		test.Equal(inode, inode2)
	}
}
