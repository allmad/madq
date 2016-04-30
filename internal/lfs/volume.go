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
	reservedArea  ReservedArea
	rootDirectory *File

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
		err = logex.Trace(bio.WriteAt(v.dev, 0, &v.reservedArea))
	}
	return err
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

func (v *Volume) getIno(name string) int {

	return 0
}

func (v *Volume) write(w *writeReq) {

}

func (v *Volume) OpenFile(name string) (*File, error) {
	ino := v.getIno(name)
	return NewFile(v, ino, name)
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
	Ino    int
	Data   []byte
	Offset int64
	Reply  chan *writeResp
}

type writeResp struct {
	N   int
	Err error
}
