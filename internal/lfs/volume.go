package lfs

import (
	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
)

var (
	ErrVolumeFileNotExists     = logex.Define("file is not exists")
	ErrVolumeFileAlreadyExists = logex.Define("file is already exists")
)

type Volume struct {
	raw    bio.RawDisker
	dev    *bio.Device
	devmgr *bio.DeviceMgr

	flow *flow.Flow

	inodeMgr *InodeMgr
	rootDir  RootDir

	writeChan chan *WriteReq
}

func NewVolume(f *flow.Flow, raw bio.RawDisker) (*Volume, error) {
	vol := &Volume{
		flow:      f.Fork(0),
		raw:       raw,
		writeChan: make(chan *WriteReq, 2),
	}

	if err := vol.init(); err != nil {
		return nil, logex.Trace(err)
	}

	vol.flow.AddOnClose(vol.Close)
	go vol.loop()
	return vol, nil
}

func (v *Volume) init() error {
	v.inodeMgr = NewInodeMgr()
	if err := v.inodeMgr.Init(v.raw); err != nil {
		return logex.Trace(err)
	}

	pointer := v.inodeMgr.GetPointer()
	v.dev = bio.NewDevice(v.raw, *pointer)
	v.devmgr = bio.NewDeviceMgr(v.flow, v.dev, pointer)

	v.inodeMgr.Start(v.devmgr)

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
			n, err := v.write(w)
			w.Reply <- &WriteResp{n, err}
		case <-v.flow.IsClose():
			break loop
		}
	}
}

func (v *Volume) getInoByName(name string) int32 {
	if name == "/" {
		return 0
	}
	return v.rootDir.Find(name)
}

func (v *Volume) write(w *WriteReq) (int, error) {
	inode, err := v.inodeMgr.GetInode(w.Ino)
	if err != nil {
		return 0, logex.Trace(err)
	}
	_ = inode
	return -1, nil
}

func (v *Volume) createFile(name string) (ino int32, err error) {
	if ino := v.rootDir.Find(name); ino >= 0 {
		return -1, ErrVolumeFileAlreadyExists.Trace()
	}
	inode, err := v.inodeMgr.NewInode()
	if err != nil {
		return -1, err
	}
	if err := v.rootDir.Add(name, inode.Ino); err != nil {
		v.inodeMgr.RemoveInode(inode.Ino)
		return -1, err
	}
	return inode.Ino, nil
}

func (v *Volume) OpenFile(name string, autoCreate bool) (*File, error) {
	var err error
	ino := v.getInoByName(name)
	if !autoCreate && ino < 0 {
		return nil, ErrVolumeFileNotExists.Trace()
	}

	// now we need to create
	if ino < 0 {
		ino, err = v.createFile(name)
		if err != nil {
			return nil, logex.Trace(err)
		}
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

type WriteReq struct {
	Ino    int32
	Data   []byte
	Offset int64
	Reply  chan *WriteResp
}

type WriteResp struct {
	N   int
	Err error
}

// InodeMgr Delegate -----------------------------------------------------------

func (v *Volume) Malloc(n int) (start int64) {
	return v.devmgr.Malloc(n)
}
