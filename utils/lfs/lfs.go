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

type checkPoint struct {
	blkOff int64
	data   map[string]int64
}

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
		cp:  &checkPoint{},
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
	return OpenFile(i, NewInode(name, i.cfg.blockSize), name)
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

func (i *Ins) readAt(f *File, p []byte, off int64) (int, error) {
	return i.rfd.ReadAt(p, off)
}

func (o *Ins) fillBuf(p []byte, extendSize int) ([]byte, int) {
	fillsize := 0
	out := len(p) & (o.cfg.blockSize - 1)
	if out > 0 {
		fillsize += o.cfg.blockSize - out
		p = append(p, o.cfg.emptyBlock[:o.cfg.blockSize-out]...)
	}
	for i := 0; i < extendSize; i++ {
		fillsize += o.cfg.blockSize
		p = append(p, o.cfg.emptyBlock...)
	}
	return p, fillsize
}

func (o *Ins) calInoRawSize(newData []byte, ino *Inode) int {
	return ino.RawSize(o.calBlockSize(len(newData)))
}

func (o *Ins) writeAt(f *File, p []byte, off int64) (int, error) {
	inoBlockSize := o.calBlockSize(o.calInoRawSize(p, f.ino))
	p, fsize := o.fillBuf(p, inoBlockSize)
	blockOff, size := o.allocBlocks(p)
	currentBlockSize := len(f.ino.Blocks)
	for i := 0; i < size; i++ {
		f.ino.Blocks = append(f.ino.Blocks, blockOff+int64(i*o.cfg.blockSize))
	}
	inoOff := len(p) - inoBlockSize*o.cfg.blockSize
	inoWriter := bytes.NewBuffer(p[inoOff:inoOff])
	if err := f.ino.PWrite(inoWriter); err != nil {
		panic(err)
	}
	// logex.Info(p)

	logex.Struct(f.ino)
	// append ino into p
	n, err := o.wfd.WriteAt(p, f.ino.Blocks[currentBlockSize])
	if err == nil && n != len(p) {
		err = ErrWriteShort.Trace(n, len(p))
	}
	if err != nil {
		f.ino.Blocks = f.ino.Blocks[:currentBlockSize]
		return n, logex.Trace(err)
	}
	return n - fsize, nil
}

func (i *Ins) Pruge() {
	i.Close()
	i.wfd.Delete()
}

func (i *Ins) Close() {
	i.wfd.Close()
	i.rfd.Close()
}
