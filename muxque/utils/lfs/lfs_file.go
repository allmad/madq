package lfs

import (
	"sync/atomic"

	"github.com/chzyer/fsmq/rpc"

	"gopkg.in/logex.v1"
)

var _ rpc.ItemStruct = new(Inode)

const (
	InoOffsetBit = 48
	InoOffsetMax = (1 << InoOffsetBit) - 1
	InoSizeBit   = 64 - InoOffsetBit
	InoSizeMax   = (1 << InoSizeBit) - 1

	InoMagic, InoMagicV2 = 0x9c, 0x80
)

var (
	InoMagicBytes      = []byte{InoMagic, InoMagicV2}
	ErrInoInvalidMagic = logex.Define("not a valid ino: magicByte not match")
)

type File struct {
	lfs  *Ins
	name string
	ino  *Inode
}

func openFile(lfs *Ins, ino *Inode, name string) (*File, error) {
	f := &File{
		lfs:  lfs,
		ino:  ino,
		name: name,
	}
	return f, nil
}

func (f *File) ReadAt(p []byte, off int64) (int, error) {
	return f.lfs.readAt(f, p, off)
}

func (f *File) WriteAt(p []byte, off int64) (int, error) {
	return f.lfs.writeAt(f, p, off)
}

func (f *File) Size() int64 {
	return atomic.LoadInt64(&f.ino.FileSize)
}

func (f *File) Close() error {
	return f.lfs.closeFile(f)
}
