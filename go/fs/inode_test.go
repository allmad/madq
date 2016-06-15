package fs

import (
	"bytes"
	"testing"

	"github.com/chzyer/test"
)

func TestInode(t *testing.T) {
	defer test.New(t)
	ino := new(Inode)

	ino.Size = 12
	for i := range ino.Offsets {
		ino.Offsets[i] = ShortAddr(i + 1)
	}

	buf := bytes.NewBuffer(nil)
	rw := NewDiskBuffer(buf)
	rw.WriteItem(ino)

	newIno := new(Inode)
	err := rw.ReadItem(newIno)
	test.Nil(err)
	test.Equal(ino, newIno)
}
