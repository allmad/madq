package fs

import (
	"bytes"
	"testing"

	"github.com/chzyer/test"
)

func TestInode(t *testing.T) {
	defer test.New(t)
	ino := NewInode(0)

	ino.Size = 12
	for i := range ino.Offsets {
		ino.Offsets[i] = ShortAddr(i + 1)
	}

	buf := bytes.NewBuffer(nil)
	rw := NewDiskBuffer(buf)
	rw.WriteItem(ino)

	newIno := NewInode(0)
	err := rw.ReadItem(newIno)
	test.Nil(err)
	test.Equal(ino, newIno)
}
