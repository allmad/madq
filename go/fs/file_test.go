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

func TestFile(t *testing.T) {
	defer test.New(t)

	delegate := &testFileDelegate{}
	NewFlusher(flow.New())
	f, err := NewFile(flow.New(), &FileConfig{
		Ino:           0,
		Flags:         os.O_CREATE,
		Delegate:      delegate,
		FlushInterval: time.Second,
		FileFlusher:   nil,
	})
	test.Nil(err)
	defer f.Close()

}
