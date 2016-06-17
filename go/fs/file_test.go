package fs

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/madq/go/bio"
	"github.com/chzyer/test"
)

type testFileDelegate struct {
	ino     int32
	lastest *Inode
	md      bio.ReadWriterAt
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

func testNewFile(md bio.ReadWriterAt) *File {
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
	return f
}

func TestFileWrite(t *testing.T) {
	defer test.New(t)

	md := test.NewMemDisk()
	f := testNewFile(md)
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

	f := testNewFile(test.NewMemDisk())
	defer f.Close()

	out := 5
	testSize := BlockSize + out
	buf := test.SeqBytes(testSize)
	testTime := 10
	for i := 0; i < testTime; i++ {
		test.Write(f, buf)
		f.Sync()
		// test.MarkLine()
	}

	fr := NewFileReader(f, 0)
	for i := 0; i < testTime; i++ {
		// test.MarkLine()
		test.Mark(i)
		test.ReadAndCheck(fr, buf)
	}

}

func TestFile(t *testing.T) {
	defer test.New(t)

	f := testNewFile(test.NewMemDisk())
	defer f.Close()

	test.Write(f, []byte("hello"))
	f.Sync()
	test.Equal(f.Size(), int64(5))
	test.ReadStringAt(f, 0, "hello")
}

func TestFileBigRW1(t *testing.T) {
	defer test.New(t)
	md := test.NewMemDisk()
	f := testNewFile(md)
	defer f.Close()

	testSize := 256<<10 + 5
	buf := test.SeqBytes(testSize)
	testTime := 2000
	for i := 0; i < testTime; i++ {
		// test.MarkLine()
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

func TestFileBigWrite(t *testing.T) {
	defer test.New(t)
	md := test.NewMemDisk()
	f := testNewFile(md)
	defer f.Close()

	const size = 1028
	data := test.RandBytes(size)
	total := 230000

	for i := 0; i < total; i++ {
		_, err := f.Write(data)
		test.Nil(err)
		switch i {
		case 220000, 223000:
			f.Sync()
			println("done", i)
		}
	}
	f.Sync()

}

func TestFileBigRW2(t *testing.T) {
	defer test.New(t)
	md := test.NewMemDisk()
	f := testNewFile(md)
	defer f.Close()

	testSize := 1048
	buf := test.SeqBytes(testSize)
	testTime := 10000
	for i := 0; i < testTime; i++ {
		// test.MarkLine()
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
