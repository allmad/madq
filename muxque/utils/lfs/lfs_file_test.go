package lfs

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/chzyer/fsmq/rpc"
	"github.com/chzyer/fsmq/utils"

	"gopkg.in/logex.v1"
)

func TestInode(t *testing.T) {
	var err error
	defer utils.TDefer(t, &err)
	InoName := rpc.NewString("inode")
	bit := uint(4)

	ino, err := NewInode(InoName, bit)
	if err != nil {
		return
	}

	if ino.blkSize != 1<<4 {
		err = logex.NewError("blkSize not expect")
		return
	}

	ino.ExtBlks(0, 12*(1<<4))
	if ino.blkOff != 12 {
		err = logex.Define("block size not expect")
		return
	}

	buf := bytes.NewBuffer(nil)
	if err = ino.PWrite(buf); err != nil {
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

	err = nil
	return
}
