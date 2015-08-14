package lfs

import (
	"github.com/chzyer/muxque/utils/bitmap"
	"gopkg.in/logex.v1"
)

const (
	BlockSize      = 1 << 12
	SegmentSizeBit = 22
)

type CheckPoint struct {
	data map[string]int64
}

// log structured file system implementation
// provide sequence-write/random-read on large topics
type Ins struct {
	cfg  *Config
	file *bitmap.File
	cp   *CheckPoint
}

type Config struct {
	BasePath       string
	BlockSize      int
	SegmentSizeBit int
}

func New(cfg *Config) (*Ins, error) {
	f, err := bitmap.NewFile(cfg.BasePath)
	if err != nil {
		return nil, logex.Trace(err)
	}
	ins := &Ins{
		cfg:  cfg,
		file: f,
	}
	go ins.readloop()
	go ins.writeloop()
	return ins, nil
}

func (i *Ins) readloop() {

}

func (i *Ins) writeloop() {

}

func (i *Ins) Open(name string) (*File, error) {
	return OpenFile(i, nil, name)
}

func (i *Ins) getFreeBlkOffset() int64 {
	return i.file.Size()
}

func (i *Ins) read(b []byte) {

}

func (i *Ins) write(b []byte) {

}
