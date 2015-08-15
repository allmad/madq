package lfs

import (
	"encoding/binary"
	"io"

	"gopkg.in/logex.v1"

	"github.com/chzyer/muxque/rpc"
)

var _ rpc.ItemStruct = new(Inode)

type Inode struct {
	Size   int64
	Blocks []int64
}

func (ino *Inode) PRead(r io.Reader) error {
	ps, err := rpc.ReadInt64(r)
	if err != nil {
		return logex.Trace(err)
	}
	ino.Size = ps.Int64()
	var length uint16
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return logex.Trace(err)
	}
	ino.Blocks = make([]int64, int(length))
	for i := 0; i < len(ino.Blocks); i++ {
		ps, err = rpc.ReadInt64(r)
		if err != nil {
			return logex.Trace(err, i)
		}
		ino.Blocks[i] = ps.Int64()
	}
	return nil
}

func (ino *Inode) PWrite(w io.Writer) (err error) {
	if err = rpc.WriteItem(w, rpc.NewInt64(uint64(ino.Size))); err != nil {
		return logex.Trace(err)
	}

	if err = binary.Write(w, binary.LittleEndian, uint16(len(ino.Blocks))); err != nil {
		return logex.Trace(err)
	}
	blks := make([]rpc.Item, len(ino.Blocks))
	for i := 0; i < len(ino.Blocks); i++ {
		blks[i] = rpc.NewInt64(uint64(ino.Blocks[i]))
	}
	if err = rpc.WriteItems(w, blks); err != nil {
		return logex.Trace(err)
	}
	return nil
}

type File struct {
	lfs  *Ins
	name string
	ino  *Inode
}

func OpenFile(lfs *Ins, ino *Inode, name string) (*File, error) {
	f := &File{
		lfs: lfs,
		ino: ino,
	}
	return f, nil
}

func (f *File) ReadAt(p []byte, off int64) (int, error) {
	return f.lfs.readAt(f, p, off)
}

func (f *File) WriteAt(p []byte, off int64) (int, error) {
	return f.lfs.writeAt(f, p, off)
}

func (f *File) Close() error {
	return f.lfs.closeFile(f)
}
