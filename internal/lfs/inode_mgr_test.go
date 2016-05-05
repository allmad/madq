package lfs

import (
	"testing"

	"github.com/chzyer/flow"
	"github.com/chzyer/madq/internal/bio"
	"github.com/chzyer/test"
)

func TestInodeMgr(t *testing.T) {
	defer test.New(t)

	test.CleanTmp()
	block, err := bio.NewFile(test.Root())
	test.Nil(err)

	im := NewInodeMgr()

	{
		err := im.Init(block)
		test.Nil(err)
	}
	ptr := im.GetPointer()

	f := flow.New()
	devmgr := bio.NewDeviceMgr(f, bio.NewDevice(block, *ptr), ptr)
	im.Start(devmgr)

	{
		inode, err := im.newInode()
		test.Nil(err)
		test.Equal(inode.Ino, 1)
		inode2, err := im.GetInode(inode.Ino)
		test.Nil(err)
		test.Equal(inode, inode2)
		err = im.removeInode(inode.Ino)
		test.Nil(err)
		inode3, err := im.GetInode(inode.Ino)
		test.Nil(inode3)
		test.Equal(err, ErrInodeMgrInodeNotFound)
	}

	{
		inode, err := im.newInode()
		test.Nil(err)
		test.Equal(inode.Ino, 1)
		{ // save position, and flush
			test.Nil(im.Flush())
		}

		im := NewInodeMgr()
		{
			err := im.Init(block)
			test.Nil(err)
			ptr := im.GetPointer()
			dev := bio.NewDevice(block, *ptr)
			im.Start(bio.NewDeviceMgr(f, dev, ptr))
		}
		inode2, err := im.GetInode(inode.Ino)
		test.Nil(err)
		test.Equal(inode, inode2)
	}
}
