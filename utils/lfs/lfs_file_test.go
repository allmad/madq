package lfs

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"gopkg.in/logex.v1"

	"github.com/chzyer/muxque/utils"
)

func TestInode(t *testing.T) {
	var err error
	defer utils.TDefer(t, &err)
	InoName := "inode"
	bit := uint(4)

	ino := NewInode(InoName, bit)

	if ino.blkSize != 1<<4 {
		err = logex.NewError("blkSize not expect")
		return
	}

	ino.ExtBlks(0, 12, nil)
	if ino.BlkSize() != 12 {
		err = logex.Define("block size not expect")
		return
	}

	if !ino.HasBlk(11) {
		err = logex.Define("block size not expect")
		return
	}

	buf := bytes.NewBuffer(nil)
	if err = ino.PWrite(buf); err != nil {
		return
	}

	if ino.PSize() != buf.Len() {
		err = fmt.Errorf("PSize not work as expect, %v, %v",
			ino.PSize(), buf.Len(),
		)
		return
	}

	ino2, err := ReadInode(buf, bit)
	if err != nil {
		return
	}
	if !reflect.DeepEqual(ino, ino2) {
		err = logex.Define("ino not expect")
		return
	}

	ino2.TrunBlk(2)
	if ino2.BlkSize() != 2 {
		err = logex.Define("trun not work as expect")
		return
	}

	ino.String()
	err = nil
	return
}
