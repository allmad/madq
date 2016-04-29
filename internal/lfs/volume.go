package lfs

import (
	"github.com/chzyer/flow"
	"github.com/chzyer/madq/internal/blockio"
)

type Volume struct {
	dev       blockio.Device
	base      string
	flow      *flow.Flow
	writeChan chan *writeReq
}

func NewVolume(f *flow.Flow, base string, dev blockio.Device) *Volume {
	vol := &Volume{
		dev:       dev,
		base:      base,
		writeChan: make(chan *writeReq, 2),
	}
	f.ForkTo(&vol.flow, vol.Close)
	return vol
}

func (v *Volume) OpenFile(name string) (*File, error) {
	return NewFile(v, name)
}

func (v *Volume) Close() {
	v.flow.Close()
}

// -----------------------------------------------------------------------------

type writeReq struct {
	Data   []byte
	Offset int64
	Reply  chan *writeResp
}

type writeResp struct {
	N   int
	Err error
}
