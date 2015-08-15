package lfs

import (
	"encoding/binary"
	"io"

	"gopkg.in/logex.v1"

	"github.com/chzyer/muxque/rpc"
)

var _ rpc.ItemStruct = new(Inode)

type Inode struct {
	blockSize int64
	Name      *rpc.String
	Blocks    []int64
}

func NewInode(name string, blockSize int) *Inode {
	return &Inode{Name: rpc.NewString(name), blockSize: int64(blockSize)}
}

func (ino *Inode) calBlockOffSize(size int) int {
	return size * 8
}

func (ino *Inode) RawSize(newBlockSize int) int {
	return ino.calBlockOffSize(newBlockSize + len(ino.Blocks))
}

func (ino *Inode) FileSize() int64 {
	return int64(len(ino.Blocks)) * ino.blockSize
}

func (ino *Inode) PSize() int {
	return 2 + // block length
		8*len(ino.Blocks) + // block size
		ino.Name.PSize()
}

func (ino *Inode) PRead(r io.Reader) (err error) {
	ino.Name, err = rpc.ReadString(r)
	if err != nil {
		return logex.Trace(err)
	}
	ps, err := rpc.ReadInt64(r)
	if err != nil {
		return logex.Trace(err)
	}
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
	if err = rpc.WriteItem(w, ino.Name); err != nil {
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
