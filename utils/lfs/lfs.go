package lfs

import (
	"sync"

	"github.com/chzyer/muxque/utils"
	"github.com/chzyer/muxque/utils/bitmap"
	"gopkg.in/logex.v1"
)

const (
	BlockSize      = 1 << 12
	SegmentSizeBit = 22
)

type checkPoint struct {
	blkOff int64
	data   map[string]int64
}

// log structured file system implementation
// provide sequence-write/random-read on large topics
type Ins struct {
	cfg *Config
	cp  *checkPoint
	rfd *bitmap.File
	wfd *bitmap.File

	ofs      map[string]*File
	ofsGuard sync.RWMutex
}

type Config struct {
	BasePath       string
	BlockSize      int
	SegmentSizeBit int
}

func (c *Config) init() {
	if c.BlockSize == 0 {
		c.BlockSize = 1 << 12
	}
	if c.SegmentSizeBit == 0 {
		c.SegmentSizeBit = 22
	}
}

func New(cfg *Config) (*Ins, error) {
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
	return OpenFile(i, nil, name)
}

func (i *Ins) getFreeBlkOffset() int64 {
	return i.cp.blkOff
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

func (i *Ins) writeAt(f *File, p []byte, off int64) (int, error) {
	return i.wfd.WriteAt(p, off)
}

func (i *Ins) Pruge() {
	i.Close()
	i.wfd.Delete()
}

func (i *Ins) Close() {
	i.wfd.Close()
	i.rfd.Close()
}
