package lfs

import (
	"testing"

	"gopkg.in/logex.v1"

	"github.com/chzyer/muxque/utils"
)

func TestInode(t *testing.T) {
	var err error
	defer utils.TDefer(t, &err)
	InoName := "inode"

	ino := NewInode(InoName, 4)
	ino.ExtBlks(0, 12, nil)
	if ino.BlkSize() != 1 {
		err = logex.NewError("block size not expect")
		return
	}

}
