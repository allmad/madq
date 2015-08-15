package lfs

import (
	"encoding/binary"
	"fmt"
	"io"

	"gopkg.in/logex.v1"

	"github.com/chzyer/muxque/rpc"
)

var _ rpc.ItemStruct = new(Inode)

const (
	InoOffsetBit = 48
	InoOffsetMax = (1 << InoOffsetBit) - 1
	InoSizeBit   = 64 - InoOffsetBit
	InoSizeMax   = (1 << InoSizeBit) - 1
)

type Inode struct {
	blockSize int64
	Name      *rpc.String
	skipOff   int64 // something deleted
	blks      []int64
}

func NewInode(name string, blockSize int) *Inode {
	return &Inode{Name: rpc.NewString(name), blockSize: int64(blockSize)}
}

func (ino *Inode) String() string {
	blks := make([]string, len(ino.blks))
	for i := range blks {
		blks[i] = fmt.Sprintf("{off:%v,size:%v}",
			ino.getOff(ino.blks[i]),
			ino.getSize(ino.blks[i]),
		)
	}
	return fmt.Sprintf("{%v, skip:%v, blks: %v}", ino.Name, ino.skipOff, blks)
}

// build a cache?
func (ino *Inode) GetRawOff(offset int64) (int, int64) {
	var size int64
	offset -= ino.skipOff

	for i := 0; i < len(ino.blks); i++ {
		size = int64(ino.getSize(ino.blks[i]))
		if offset < size {
			return i, ino.getOff(ino.blks[i]) + offset
		}
		offset -= size
	}
	return -1, -1
}

func (ino *Inode) getSize(blkInf int64) int {
	return int(ino.blockSize) - int(blkInf>>InoOffsetBit)
}

func (ino *Inode) getOff(blkInf int64) int64 {
	return blkInf & InoOffsetMax
}

func (ino *Inode) GetBlk(i int) (offset int64, size int) {
	blockInfo := ino.blks[i]
	offset = ino.getOff(blockInfo)
	size = ino.getSize(blockInfo)
	return
}

func (ino *Inode) TrunBlk(i int) {
	ino.blks = ino.blks[:i]
}

func (ino *Inode) ExtBlks(blockOff int64, size int, remains [][2]int) {
	var out int64 = 0
	p := 0
	for i := 0; i < size; i++ {
		out = 0
		if p < len(remains) && remains[p][0] == i {
			out = int64(remains[p][1])
			p++
		}
		block := ino.GenBlock(blockOff+int64(i)*ino.blockSize, out)
		ino.blks = append(ino.blks, block)
	}
}

func (ino *Inode) BlockSize() int {
	return len(ino.blks)
}

func (ino *Inode) GenBlock(offset, out int64) (info int64) {
	if offset > 0 {
		info |= offset & InoOffsetMax
	}
	if out > 0 {
		info |= ((ino.blockSize - (out & InoSizeMax)) << InoOffsetBit)
	}
	return
}

func (ino *Inode) calBlockOffSize(size int) int {
	return size * 8
}

func (ino *Inode) RawSize(newBlockSize int) int {
	return ino.calBlockOffSize(newBlockSize + len(ino.blks))
}

func (ino *Inode) PSize() int {
	return 2 + // block length
		8*len(ino.blks) + // block size
		ino.Name.PSize() + // name
		8 // skipOff
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
	ino.skipOff = ps.Int64()

	var length uint16
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return logex.Trace(err)
	}
	ino.blks = make([]int64, int(length))
	for i := 0; i < len(ino.blks); i++ {
		ps, err = rpc.ReadInt64(r)
		if err != nil {
			return logex.Trace(err, i)
		}
		ino.blks[i] = ps.Int64()
	}
	return nil
}

func (ino *Inode) PWrite(w io.Writer) (err error) {
	if err = rpc.WriteItems(w, []rpc.Item{
		ino.Name,
		rpc.NewInt64(uint64(ino.skipOff)),
	}); err != nil {
		return logex.Trace(err)
	}
	if err = binary.Write(w, binary.LittleEndian, uint16(len(ino.blks))); err != nil {
		return logex.Trace(err)
	}
	blks := make([]rpc.Item, len(ino.blks))
	for i := 0; i < len(ino.blks); i++ {
		blks[i] = rpc.NewInt64(uint64(ino.blks[i]))
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

func (f *File) Close() error {
	return f.lfs.closeFile(f)
}
