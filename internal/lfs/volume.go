package lfs

import (
	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
)

var (
	ErrVolumeFileNotExists = logex.Define("file is not exists")
)

type Volume struct {
	raw bio.RawDisker

	flow *flow.Flow

	inodeMgr *InodeMgr
	rootDir  RootDir

	buffer []byte

	writeChan chan *writeReq
}

func NewVolume(f *flow.Flow, raw bio.RawDisker) (*Volume, error) {
	vol := &Volume{
		raw:       raw,
		inodeMgr:  NewInodeMgr(raw),
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
