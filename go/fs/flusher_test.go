package fs

import (
	"fmt"
	"testing"
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/madq/go/bio"
	"github.com/chzyer/test"
)

var _ FlushDelegate = new(testFlusherDelegate)

type testFlusherDelegate struct {
	md bio.ReadWriterAt
}

func (m *testFlusherDelegate) ReadData(addr ShortAddr, n int) ([]byte, error) {
	buf := make([]byte, n)
	n, err := m.md.ReadAt(buf, int64(addr))
	if err != nil {
		return nil, err
	}
	if n != len(buf) {
		return nil, fmt.Errorf("short read")
	}
	return buf, nil
}

func (m *testFlusherDelegate) WriteData(addr ShortAddr, b []byte) error {
	n, err := m.md.WriteAt(b, int64(addr))
	if err != nil {
		return err
	}
	if n != len(b) {
		return fmt.Errorf("short written")
	}
	return nil
}

type testInodePoolMemDiskDelegate struct {
	lastestAddr Address
	md          bio.ReadWriterAt
}

func (t *testInodePoolMemDiskDelegate) GetInode(ino int32) (*Inode, error) {
	return t.GetInodeByAddr(t.lastestAddr)
}

func (t *testInodePoolMemDiskDelegate) GetInodeByAddr(addr Address) (*Inode, error) {
	ino := NewInode(0)
	buf := make([]byte, ino.DiskSize())
	_, err := t.md.ReadAt(buf, int64(addr))
	if err != nil {
		return nil, err
	}
	if err := ino.ReadDisk(buf); err != nil {
		return nil, err
	}
	return ino, nil
}

func TestFlusherBigRW(t *testing.T) {
	defer test.New(t)

	flusherDelegate := &testFlusherDelegate{test.NewMemDisk()}
	f := flow.New()
	flusher := NewFlusher(f, &FlusherConfig{
		Interval: time.Second,
		Delegate: flusherDelegate,
		Offset:   1,
	})

	ipool0 := NewInodePool(0, nil)
	ipool0.InitInode()
	done := make(chan error, 1)
	expect := test.SeqBytes(BlockSize + 5)

	testTime := 10
	for i := 0; i < testTime; i++ {
		flusher.WriteByInode(ipool0, expect, done)
		flusher.Flush()
		test.Nil(<-done)
	}
}

func TestFlusher(t *testing.T) {
	defer test.New(t)

	flusherDelegate := &testFlusherDelegate{test.NewMemDisk()}
	f := flow.New()
	flusher := NewFlusher(f, &FlusherConfig{
		Interval: time.Second,
		Delegate: flusherDelegate,
		Offset:   1,
	})
	{
		ipool0 := NewInodePool(0, nil)
		ipool0.InitInode()
		done := make(chan error, 1)
		flusher.WriteByInode(ipool0, []byte("hello"), done)
		flusher.Flush()
		test.Nil(<-done)
	}
	{
		// println("--------------", flusher.offset)
		delegate := &testInodePoolMemDiskDelegate{
			lastestAddr: 5 + 1,
			md:          flusherDelegate.md,
		}
		ipool0 := NewInodePool(0, delegate)
		inode, err := ipool0.GetLastest()
		test.Nil(err)
		test.Equal(inode.Size, Int32(5))
		done := make(chan error, 1)
		tmpdata := test.SeqBytes((256 << 10) + 10)
		flusher.WriteByInode(ipool0, tmpdata, done)
		flusher.Flush()
		test.Nil(<-done)
		test.Equal(inode.Size, Int32((256<<10)+10+5))

		block1 := make([]byte, BlockSize)
		copy(block1, []byte("hello"))
		n := copy(block1[5:], tmpdata)
		block2 := make([]byte, len(tmpdata)-n)
		copy(block2, tmpdata[n:])
		gotBlock1 := make([]byte, len(block1))
		gotBlock2 := make([]byte, len(block2))
		test.ReadAt(delegate.md, gotBlock1, int64(inode.Offsets[0]))
		test.EqualBytes(gotBlock1, block1)
		test.ReadAt(delegate.md, gotBlock2, int64(inode.Offsets[1]))
		test.EqualBytes(gotBlock2, block2)

	}
	flusher.Close()
}
