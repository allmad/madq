package lfs

import (
	"bytes"
	"sync"

	"github.com/chzyer/muxque/utils"
	"github.com/chzyer/muxque/utils/bitmap"
	"gopkg.in/logex.v1"
)

const (
	BlockSize      = 1 << 12
	SegmentSizeBit = 22
)

var (
	ErrWriteShort = logex.Define("write too short")
)

// log structured file system implementation
// provide sequence-write/random-read on large topics
type Ins struct {
	cfg     *Config
	cp      *checkPoint
	cpGuard sync.Mutex

	rfd *bitmap.File
	wfd *bitmap.File

	ofs      map[string]*File
	ofsGuard sync.RWMutex
}

type Config struct {
	BasePath   string
	BlockBit   uint
	SegmentBit int

	blockSize  int
	emptyBlock []byte
}

func (c *Config) init() error {
	if c.BlockBit == 0 {
		c.BlockBit = 12
	}
	if c.SegmentBit == 0 {
		c.SegmentBit = 22
	}
	c.blockSize = 1 << c.BlockBit
	c.emptyBlock = make([]byte, c.blockSize)
	return nil
}

func New(cfg *Config) (*Ins, error) {
	if err := cfg.init(); err != nil {
		return nil, logex.Trace(err)
	}
	rfd, err := bitmap.NewFile(cfg.BasePath)
	if err != nil {
		return nil, logex.Trace(err)
	}
	wfd, err := bitmap.NewFile(cfg.BasePath)
	if err != nil {
		return nil, logex.Trace(err)
	}

	ins := &Ins{
		cfg: cfg,
		rfd: rfd,
		wfd: wfd,
		cp:  newCheckPoint(),
		ofs: make(map[string]*File),
	}
	go ins.readloop()
	go ins.writeloop()
	return ins, nil
}

func (i *Ins) readloop() {

}

func (i *Ins) writeloop() {

}

func (i *Ins) OpenReader(name string) (*utils.Reader, error) {
	f, err := i.Open(name)
	if err != nil {
		return nil, err
	}
	return &utils.Reader{f, 0}, nil
}

func (i *Ins) OpenWriter(name string) (*utils.Writer, error) {
	f, err := i.Open(name)
	if err != nil {
		return nil, err
	}
	return &utils.Writer{f, 0}, nil
}

func (i *Ins) Open(name string) (*File, error) {
	i.ofsGuard.RLock()
	f := i.ofs[name]
	i.ofsGuard.RUnlock()
	if f != nil {
		return f, nil
	}

	i.ofsGuard.Lock()
	f, err := openFile(i, NewInode(name, i.cfg.blockSize), name)
	if err == nil {
		i.ofs[name] = f
	}
	i.ofsGuard.Unlock()
	return f, logex.Trace(err)
}

func (i *Ins) calBlockSize(size int) int {
	blockSize := size >> i.cfg.BlockBit
	if size&(i.cfg.blockSize-1) != 0 {
		blockSize++
	}
	return blockSize
}

// return offsets
func (i *Ins) allocBlocks(p []byte) (startOff int64, size int) {
	blockSize := i.calBlockSize(len(p))
	i.cpGuard.Lock()
	startOff = i.cp.blkOff
	i.cp.blkOff += int64(blockSize) * int64(i.cfg.blockSize)
	i.cpGuard.Unlock()
	return startOff, blockSize
}

func (i *Ins) closeFile(f *File) error {
	i.ofsGuard.Lock()
	delete(i.ofs, f.name)
	i.ofsGuard.Unlock()
	return nil
}

// 1. len(p) <= blk
// 2. 2*blk > len(p) > blk, continuous
// 3. 2*blk > len(p) > blk, not continuous
func (o *Ins) readAt(f *File, p []byte, off int64) (int, error) {
	blkIdx, rawOff := f.ino.GetRawOff(off)
	blkOff, blkSize := f.ino.GetBlk(blkIdx)
	pOff := 0
	pEnd := 0
	bytesRead := 0

	// try to find the large continuous blocks
	for bytesRead < len(p) {
		pEnd += blkSize
		if pEnd >= len(p) {
			pEnd = len(p) // last one
		} else {
			lastBlkEnd := blkOff + int64(blkSize)
			blkIdx++
			blkOff, blkSize = f.ino.GetBlk(blkIdx)
			if blkOff == lastBlkEnd { // continuous
				continue
			}
		}

		n, err := o.rfd.ReadAt(p[pOff:pEnd], rawOff)
		bytesRead += n
		if err != nil {
			return bytesRead, logex.Trace(err)
		}

		rawOff = blkOff
		pOff = pEnd
	}
	return bytesRead, nil
}

func (o *Ins) fillBuf(p []byte, extendSize int) ([]byte, int, int) {
	fillsize := 0
	remain := len(p) & (o.cfg.blockSize - 1)
	if remain > 0 {
		fillsize += o.cfg.blockSize - remain
		p = append(p, o.cfg.emptyBlock[:o.cfg.blockSize-remain]...)
	}
	for i := 0; i < extendSize; i++ {
		fillsize += o.cfg.blockSize
		p = append(p, o.cfg.emptyBlock...)
	}
	return p, fillsize, remain
}

func (o *Ins) calInoRawSize(newData []byte, ino *Inode) int {
	return ino.RawSize(o.calBlockSize(len(newData)))
}

func (o *Ins) plusBlkOffset(start int64, size int) int64 {
	return start + int64(size)<<o.cfg.BlockBit
}

func (o *Ins) writeAt(f *File, p []byte, off int64) (int, error) {
	// calculate inode staff
	inoRawSize := o.calInoRawSize(p, f.ino)
	inoBlockSize := o.calBlockSize(inoRawSize)
	// inoRemain := inoRawSize & (o.cfg.blockSize - 1)

	// fill buffer, and alloc it
	p, fsize, remain := o.fillBuf(p, inoBlockSize)
	remain = remain
	blockOff, size := o.allocBlocks(p)

	curBlkSize := f.ino.BlockSize()
	f.ino.ExtBlks(blockOff, size-inoBlockSize, [][2]int{
		{size - 1 - inoBlockSize, remain},
	})

	// start writing ino
	inoOff := len(p) - inoBlockSize*o.cfg.blockSize
	if err := f.ino.PWrite(bytes.NewBuffer(p[inoOff:inoOff])); err != nil {
		panic(err)
	}

	// logex.Info(f.ino)

	// write to fs
	woff, _ := f.ino.GetBlk(curBlkSize)
	n, err := o.wfd.WriteAt(p, woff)
	if err == nil && n != len(p) {
		err = ErrWriteShort.Trace(n, len(p))
	}
	if err != nil {
		// revent ino
		f.ino.TrunBlk(curBlkSize)
		return n, logex.Trace(err)
	}
	o.cp.SetInoOffset(f.name, woff+int64(inoOff))
	return n - fsize, nil
}

func (i *Ins) Pruge() {
	i.Close()
	i.wfd.Delete()
}

func (i *Ins) Close() {
	i.rfd.Close()
	if err := i.cp.Save(&utils.Writer{i.wfd, i.wfd.Size()}); err != nil {
		logex.Error(err)
	}
	i.wfd.Close()
}
