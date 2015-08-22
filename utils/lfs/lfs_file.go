package lfs

import (
	"bytes"
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

	InoMagic, InoMagicV2 = 0x9c, 0x80
)

var (
	InoMagicBytes      = []byte{InoMagic, InoMagicV2}
	ErrInoInvalidMagic = logex.Define("not a valid ino: magicByte not match")
)

type Inode struct {
	blkBit  uint
	blkSize int64
	Name    *rpc.String
	skipOff int64 // something deleted
	blks    []int64
}

func ReadInode(r io.Reader, blkBit uint) (*Inode, error) {
	ino := &Inode{
		blkBit:  blkBit,
		blkSize: 1 << blkBit,
	}
	if err := ino.PRead(r); err != nil {
		return nil, logex.Trace(err)
	}
	return ino, nil
}

func NewInode(name string, blkBit uint) *Inode {
	return &Inode{
		Name:    rpc.NewString(name),
		blkBit:  blkBit,
		blkSize: 1 << blkBit,
	}
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

// TODO: build a cache?
func (ino *Inode) GetRawOff(offset int64) (int, int64) {
	maxSize := int64(len(ino.blks)) << ino.blkBit
	if offset > maxSize {
		return -1, -1
	}
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
	return int(ino.blkSize) - int(blkInf>>InoOffsetBit)
}

func (ino *Inode) getOff(blkInf int64) int64 {
	return blkInf & InoOffsetMax
}

func (ino *Inode) HasBlk(i int) bool {
	return i >= 0 && i < len(ino.blks)
}

func (ino *Inode) GetBlk(i int) (offset int64, size int) {
	blkInfo := ino.blks[i]
	offset = ino.getOff(blkInfo)
	size = ino.getSize(blkInfo)
	return
}

func (ino *Inode) TrunBlk(i int) {
	ino.blks = ino.blks[:i]
}

func (ino *Inode) ExtBlks(blkOff int64, size int, remains [][2]int) {
	var out int64 = 0
	p := 0
	for i := 0; i < size; i++ {
		out = 0
		if p < len(remains) && remains[p][0] == i {
			out = int64(remains[p][1])
			p++
		}
		blk := ino.GenBlk(blkOff+int64(i)<<ino.blkBit, out)
		ino.blks = append(ino.blks, blk)
	}
}

func (ino *Inode) BlkSize() int {
	return len(ino.blks)
}

func (ino *Inode) GenBlk(offset, out int64) (info int64) {
	if offset > 0 {
		info |= offset & InoOffsetMax
	}
	if out > 0 {
		info |= ((ino.blkSize - (out & InoSizeMax)) << InoOffsetBit)
	}
	return
}

func (ino *Inode) calBlkOffSize(size int) int {
	return size * 8
}

func (ino *Inode) RawSize(newBlkSize int) int {
	return ino.calBlkOffSize(newBlkSize + len(ino.blks))
}

func (ino *Inode) PRead(r io.Reader) (err error) {
	var (
		length uint16
		ps     *rpc.Int64
	)
	buf := make([]byte, len(InoMagicBytes))
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return logex.Trace(err)
	}
	if !bytes.Equal(buf, InoMagicBytes) {
		return ErrInoInvalidMagic.Trace(buf)
	}
	if err = binary.Read(r, binary.LittleEndian, &length); err != nil {
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

	ino.Name, err = rpc.ReadString(r)
	if err != nil {
		return logex.Trace(err)
	}

	ps, err = rpc.ReadInt64(r)
	if err != nil {
		return logex.Trace(err)
	}
	ino.skipOff = ps.Int64()
	return nil
}

func (ino *Inode) PWrite(w io.Writer) (err error) {
	if _, err := w.Write(InoMagicBytes); err != nil {
		return logex.Trace(err)
	}
	if err = binary.Write(w, binary.LittleEndian, uint16(len(ino.blks))); err != nil {
		return logex.Trace(err)
	}
	blks := make([]rpc.Item, len(ino.blks)+2)
	for i := 0; i < len(ino.blks); i++ {
		blks[i] = rpc.NewInt64(uint64(ino.blks[i]))
	}
	blks[len(ino.blks)] = ino.Name
	blks[len(ino.blks)+1] = rpc.NewInt64(uint64(ino.skipOff))

	if err = rpc.WriteItems(w, blks); err != nil {
		return logex.Trace(err)
	}
	return nil
}

func (ino *Inode) PSize() int {
	return 2 + // ino magic byte
		ino.Name.PSize() + // name
		1 + 8 + // magic + skipOff
		2 + (1+8)*len(ino.blks) // magic + length(uint16) +
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

func (f *File) Size() int64 {
	return 0
}

func (f *File) Close() error {
	return f.lfs.closeFile(f)
}
