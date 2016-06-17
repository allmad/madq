package fs

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/test"
)

type testFileDelegate struct {
	ino     int32
	lastest *Inode
	md      *test.MemDisk
}

func (t *testFileDelegate) ReadData(addr ShortAddr, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := t.md.ReadAt(buf, int64(addr))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (t *testFileDelegate) GetInode(ino int32) (*Inode, error) {
	t.ino = ino
	var err error
	if t.lastest == nil {
		err = io.EOF
	}
	return t.lastest, err
}

func (t *testFileDelegate) GetInodeByAddr(addr Address) (*Inode, error) {
	ino := NewInode(t.ino)
	if err := ReadDisk(t.md, ino, addr); err != nil {
		return nil, err
	}
	return ino, nil
}

func testNewFile() (*File, *test.MemDisk) {
	md := test.NewMemDisk(0)
	delegate := &testFileDelegate{md: md}
	flusherDelegate := &testFlusherDelegate{md: md}

	flusher := NewFlusher(flow.New(), &FlusherConfig{
		Interval: time.Second,
		Offset:   1,
		Delegate: flusherDelegate,
	})

	f, err := NewFile(flow.New(), &FileConfig{
		Ino:           0,
		Flags:         os.O_CREATE,
		Delegate:      delegate,
		FlushInterval: time.Second,
		FileFlusher:   flusher,
	})
	test.Nil(err)
	return f, md
}

func TestFileWrite(t *testing.T) {
	defer test.New(t)

	f, md := testNewFile()
	defer f.Close()

	out := 5
	testSize := BlockSize + out
	buf := test.SeqBytes(testSize)
	testTime := 3
	for i := 0; i < testTime; i++ {
		test.Write(f, buf)
		f.Sync()
		// test.MarkLine()
	}

	got := make([]byte, testSize)
	inodeBuf := make([]byte, InodeSize)
	md.SeekRead(1, 0) // 1: offset

	// 1
	test.Read(md, got)
	test.EqualBytes(got, buf)
	ino := NewInode(0)
	test.Read(md, inodeBuf)
	test.Nil(ino.ReadDisk(inodeBuf))
	test.ReadAndCheck(md, MagicEOF)
	test.True(ino.Offsets[0] == 1)
	test.True(ino.Offsets[1] == BlockSize+1)

	// 2
	margin := out
	off2 := md.SeekRead(0, 0)
	{
		tmp := make([]byte, len(buf)+out)
		test.Read(md, tmp)
		test.EqualBytes(tmp[:margin], buf[len(buf)-margin:]) // copy last partial block
		test.EqualBytes(tmp[margin:], buf)                   // whole buf
		test.Read(md, inodeBuf)
		test.Nil(ino.ReadDisk(inodeBuf))
		test.True(ino.Offsets[0] == 1)
		test.True(ino.Offsets[1] == ShortAddr(off2))
		test.True(ino.Offsets[2] == ShortAddr(off2+BlockSize))
		test.ReadAndCheck(md, MagicEOF)
	}

	// 3
	off3 := md.SeekRead(0, 0)
	{
		margin = 2 * out
		tmp := make([]byte, len(buf)+margin)
		test.Read(md, tmp)
		test.EqualBytes(tmp[:margin], buf[len(buf)-margin:])
		test.EqualBytes(tmp[margin:], buf)
		test.Read(md, inodeBuf)
		test.Nil(ino.ReadDisk(inodeBuf))
		test.True(ino.Offsets[0] == 1)
		test.True(ino.Offsets[1] == ShortAddr(off2))
		test.True(ino.Offsets[2] == ShortAddr(off3))
		test.True(ino.Offsets[3] == ShortAddr(off3+BlockSize))
	}
}

func TestFileRead(t *testing.T) {
	defer test.New(t)

	f, _ := testNewFile()
	defer f.Close()

	out := 5
	testSize := BlockSize + out
	buf := test.SeqBytes(testSize)
	testTime := 3
	for i := 0; i < testTime; i++ {
		test.Write(f, buf)
		f.Sync()
		// test.MarkLine()
	}

	fr := NewFileReader(f, 0)
	for i := 0; i < testTime; i++ {
		test.Mark(i)
		test.ReadAndCheck(fr, buf)
	}

	// go/fs/file_test.go:150: [info:2] equal [262149]byte in [0, 16]:
	//     [251 252 253 254 255 0 1 2 3 4 0 1 2 3 4 5]
	//     [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15]

}

func TestFile(t *testing.T) {
	defer test.New(t)

	f, _ := testNewFile()
	defer f.Close()

	test.Write(f, []byte("hello"))
	f.Sync()
	test.Equal(f.Size(), int64(5))
	test.ReadStringAt(f, 0, "hello")
}

func TestFileBigRW(t *testing.T) {
	defer test.New(t)
	f, _ := testNewFile()
	defer f.Close()

	testSize := 256<<10 + 5
	buf := test.SeqBytes(testSize)
	testTime := 10
	for i := 0; i < testTime; i++ {
		test.Write(f, buf)
		f.Sync()
	}
	test.Mark("fileSize")
	test.Equal(f.Size(), testSize*testTime)

	fr := NewFileReader(f, 0)
	for i := 0; i < testTime; i++ {
		test.Mark("read ", i)
		test.ReadAndCheck(fr, buf)
	}
}
