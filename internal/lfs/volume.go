package lfs

import (
	"io"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
)

type Volume struct {
	dev  bio.Device
	flow *flow.Flow

	// reserved area
	reservedArea ReservedArea

	writeChan chan *writeReq
}

func NewVolume(f *flow.Flow, dev bio.Device) (*Volume, error) {
	vol := &Volume{
		dev:          dev,
		reservedArea: NewReservedArea(),
		writeChan:    make(chan *writeReq, 2),
	}
	if err := vol.initReservedArea(); err != nil {
		return nil, logex.Trace(err)
	}
	f.ForkTo(&vol.flow, vol.Close)
	go vol.loop()
	return vol, nil
}

func (v *Volume) initReservedArea() error {
	err := bio.ReadAt(v.dev, 0, &v.reservedArea)
	if err != nil && logex.Equal(err, io.EOF) {
		err = nil
	}
	return nil
}

func (v *Volume) loop() {
	v.flow.Add(1)
	defer v.flow.DoneAndClose()

loop:
	for {
		select {
		case w := <-v.writeChan:
			v.write(w)
		case <-v.flow.IsClose():
			break loop
		}
	}
}

func (v *Volume) write(w *writeReq) {

}

func (v *Volume) OpenFile(name string) (*File, error) {
	return NewFile(v, name)
}

func (v *Volume) Close() {
	if !v.flow.MarkExit() {
		return
	}
	// clean in writeChan
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
