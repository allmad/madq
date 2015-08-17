package lfs

import (
	"bytes"
	"io"
	"sync"

	"github.com/chzyer/muxque/utils"
	"github.com/chzyer/muxque/utils/bitmap"
	"gopkg.in/logex.v1"
)

const (
	BlkBit     = 12
	SegmentBit = 22
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
	BlkBit     uint
	SegmentBit int

	blkSize  int
	emptyBlk []byte
}

func (c *Config) init() error {
	if c.BlkBit == 0 {
		c.BlkBit = BlkBit
	}
	if c.SegmentBit == 0 {
		c.SegmentBit = SegmentBit
	}
	c.blkSize = 1 << c.BlkBit
	c.emptyBlk = make([]byte, c.blkSize)
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
	f, err := openFile(i, NewInode(name, i.cfg.BlkBit), name)
	if err == nil {
		i.ofs[name] = f
	}
	i.ofsGuard.Unlock()
	return f, logex.Trace(err)
}

func (i *Ins) calBlkSize(size int) int {
	blkSize := size >> i.cfg.BlkBit
	if size&(i.cfg.blkSize-1) != 0 {
		blkSize++
	}
	return blkSize
}

// return offsets
func (i *Ins) allocBlks(p []byte) (startOff int64, size int) {
	blkSize := i.calBlkSize(len(p))
	i.cpGuard.Lock()
	startOff = i.cp.blkOff
	i.cp.blkOff += int64(blkSize) * int64(i.cfg.blkSize)
	i.cpGuard.Unlock()
	return startOff, blkSize
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
	if blkIdx < 0 {
		return 0, io.EOF
	}
	blkOff, blkSize := f.ino.GetBlk(blkIdx)
	pOff := 0
	pEnd := 0
	bytesRead := 0

	// println()
	// try to find the large continuous blocks
	for bytesRead < len(p) {
		pEnd += blkSize
		if pEnd == blkSize { // first time, maybe some offset exists
			pEnd -= int(rawOff - blkOff)
		}
		if pEnd >= len(p) {
			pEnd = len(p) // last one
		} else {
			lastBlkEnd := blkOff + int64(blkSize)
			blkIdx++
			if f.ino.HasBlk(blkIdx) {
				blkOff, blkSize = f.ino.GetBlk(blkIdx)
				if blkOff == lastBlkEnd { // continuous
					continue
				}
			} else {
				blkOff, blkSize = -1, -1
			}
		}

		// println("readat:", pOff, pEnd, rawOff, pEnd-pOff, len(p), off)
		n, err := o.rfd.ReadAt(p[pOff:pEnd], rawOff)
		bytesRead += n
		if err != nil {
			return bytesRead, logex.Trace(err)
		}
		if blkOff == -1 {
			return bytesRead, logex.Trace(io.EOF)
		}

		rawOff = blkOff
		pOff = pEnd
	}
	return bytesRead, nil
}

func (o *Ins) fillBuf(p []byte, extendSize int) ([]byte, int, int) {
	fillsize := 0
	remain := len(p) & (o.cfg.blkSize - 1)
	if remain > 0 {
		fillsize += o.cfg.blkSize - remain
		p = append(p, o.cfg.emptyBlk[:o.cfg.blkSize-remain]...)
	}
	for i := 0; i < extendSize; i++ {
		fillsize += o.cfg.blkSize
		p = append(p, o.cfg.emptyBlk...)
	}
	return p, fillsize, remain
}

func (o *Ins) calInoRawSize(newData []byte, ino *Inode) int {
	return ino.RawSize(o.calBlkSize(len(newData)))
}

func (o *Ins) plusBlkOffset(start int64, size int) int64 {
	return start + int64(size)<<o.cfg.BlkBit
}

func (o *Ins) writeAt(f *File, p []byte, off int64) (int, error) {
	// calculate inode staff
	inoRawSize := o.calInoRawSize(p, f.ino)
	inoBlkSize := o.calBlkSize(inoRawSize)

	// fill buffer, and alloc it
	p, fsize, remain := o.fillBuf(p, inoBlkSize)
	remain = remain
	blkOff, size := o.allocBlks(p)

	curBlkSize := f.ino.BlkSize()
	f.ino.ExtBlks(blkOff, size-inoBlkSize, [][2]int{
		{size - 1 - inoBlkSize, remain},
	})

	// start writing ino
	inoOff := len(p) - inoBlkSize*o.cfg.blkSize
	if err := f.ino.PWrite(bytes.NewBuffer(p[inoOff:inoOff])); err != nil {
		panic(err)
	}

	//println(f.ino.String())

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
	// logex.Info(i.cp)
	i.rfd.Close()
	if err := i.cp.Save(&utils.Writer{i.wfd, i.wfd.Size()}); err != nil {
		logex.Error(err)
	}
	i.wfd.Close()
}
