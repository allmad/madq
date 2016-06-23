package fs

import (
	"io"
	"testing"

	"github.com/chzyer/test"
)

type testInodePoolDelegate struct {
	lastest *Inode
	data    map[Address]*Inode
}

func (t *testInodePoolDelegate) SaveInode(ino *Inode) {}

func (t *testInodePoolDelegate) GetInode(ino int32) (*Inode, error) {
	if t.lastest != nil {
		return t.lastest, nil
	}
	return nil, io.EOF
}

func (t *testInodePoolDelegate) GetInodeByAddr(addr Address) (*Inode, error) {
	ino, ok := t.data[addr]
	if ok {
		return ino, nil
	}
	return nil, io.EOF
}

func checkInoOffset(ino *Inode, start ShortAddr, n int) {
	val := start
	for i := 0; i < n; i++ {
		test.Equal(ino.Offsets[i], val)
		val++
	}
}

func TestInodePool(t *testing.T) {
	defer test.New(t)
	delegate := &testInodePoolDelegate{
		data: make(map[Address]*Inode),
	}
	ip := NewInodePool(0, delegate)
	ip.InitInode()

	lastest, err := ip.GetLastest()
	test.Nil(err)
	test.True(lastest.Size == 0)

	n := len(lastest.Offsets) + 1

	for i := 0; i < n; i++ {
		inode, idx, err := ip.RefPayloadBlock()
		test.Nil(err)
		inode.SetOffset(idx, ShortAddr(i), BlockSize)
	}

	nextN := func() ShortAddr {
		ret := n
		n++
		return ShortAddr(ret)
	}

	// start: 0
	ino0, err := ip.getInScatter(1)
	test.Nil(err)
	// Offsets: [0, ..., 149]
	checkInoOffset(ino0, 0, InodeBlockCnt)
	test.True(ino0.Start == 0)
	test.True(ino0.Size == InodeCap)

	// start: 1
	ino1, err := ip.getInScatter(0)
	test.Nil(err)
	test.True(ino1.Start == InodeBlockCnt)
	test.True(ino1.Size == BlockSize)
	checkInoOffset(ino1, InodeBlockCnt, 1)
	test.True(ino0.addr == *ino1.PrevInode[0])

	// flush to disk
	// 1. addrs are changed
	// 2. pool must changed
	ip.OnFlush(ino0, 1)
	ip.OnFlush(ino1, 2)

	test.True(ino0.addr == *ino1.PrevInode[0])
	test.Equal(ip.getInPool(ino0.addr), ino0)
	test.Equal(ip.getInPool(ino1.addr), ino1)

	// write to ino1
	inode, idx, err := ip.RefPayloadBlock()
	test.Nil(err)
	inode.SetOffset(idx, nextN(), 5)
	checkInoOffset(ino1, InodeBlockCnt, 2)
	test.Equal(inode, ino1)

	ip.OnFlush(ino1, 3)
	test.Equal(ip.getInPool(Address(2)), (*Inode)(nil))
	test.Equal(ip.getInPool(Address(3)), ino1)

	// test restore
	delegate.data = ip.pool
	delegate.lastest = ino1

	ip = NewInodePool(0, delegate)
	// write to ino1
	{
		inode, idx, err := ip.RefPayloadBlock()
		test.Nil(err)
		test.Equal(idx, 1)
		inode.SetOffset(idx, nextN(), BlockSize-5)
		inode, idx, err = ip.RefPayloadBlock()
		test.Nil(err)
		test.Equal(idx, 2)
		inode.SetOffset(idx, nextN(), 5)
	}
	{ // ino0
		inode, err := ip.getInScatter(1)
		test.Nil(err)
		test.Equal(inode, ino0)
	}
	{ // ino1
		inode, err := ip.getInScatter(0)
		test.Nil(err)
		test.Equal(inode, ino1)
		test.True(inode.Offsets[0] == InodeBlockCnt)
		test.True(inode.Offsets[1] == InodeBlockCnt+2)
		test.True(inode.Offsets[2] == InodeBlockCnt+3)
		test.True(inode.Size == BlockSize*2+5)
	}
}

func TestInodePoolSeek(t *testing.T) {
	defer test.New(t)

	delegate := &testInodePoolDelegate{
		data: make(map[Address]*Inode),
	}
	ip := NewInodePool(0, delegate)
	ip.InitInode()

	ip.RefPayloadBlock()
}
