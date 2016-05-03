package lfs

import (
	"sync/atomic"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
)

var (
	ErrVolumeFileNotExists = logex.Define("file is not exists")
)

type Volume struct {
	raw bio.RawDisker
	dev *bio.Device

	// point to the end of data written
	pointer int64

	flow *flow.Flow

	inodeMgr *InodeMgr
	rootDir  RootDir

	writeChan chan *writeReq
}

func NewVolume(f *flow.Flow, raw bio.RawDisker) (*Volume, error) {
	vol := &Volume{
		raw:       raw,
		writeChan: make(chan *writeReq, 2),
	}

	if err := vol.init(); err != nil {
		return nil, logex.Trace(err)
	}

	f.ForkTo(&vol.flow, vol.Close)
	go vol.loop()
	return vol, nil
}

func (v *Volume) init() error {
	v.inodeMgr = NewInodeMgr(v)
	if err := v.inodeMgr.Init(v.raw); err != nil {
		return logex.Trace(err)
	}
	v.pointer = v.inodeMgr.GetPointer()
	v.dev = bio.NewDevice(v.raw, v.pointer)
	v.inodeMgr.Start(v.dev)

	rootDir, err := NewRootDir(v)
	if err != nil {
		return logex.Trace(err)
	}
	v.rootDir = *rootDir
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

func (v *Volume) getInoByName(name string) (int, bool) {
	if name == "/" {
		return 0, true
	}
	println("name")
	return -1, false
}

func (v *Volume) write(w *writeReq) {

}

func (v *Volume) rawWrite(b []byte, offset int64) (int, error) {
	return v.raw.WriteAt(b, offset)
}

func (v *Volume) OpenFile(name string, autoCreate bool) (*File, error) {
	ino, exists := v.getInoByName(name)
	if !autoCreate && !exists {
		return nil, ErrVolumeFileNotExists.Trace()
	}
	if !exists {

	}
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

// InodeMgr Delegate -----------------------------------------------------------

func (v *Volume) Malloc(n int) (start int64) {
	return atomic.AddInt64(&v.pointer, int64(n)) - int64(n)
}

func (v *Volume) MallocWriter(n int) *bio.Writer {
	return v.dev.GetWriter(v.Malloc(n), n)
}
