package lfs

import (
	"fmt"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
)

var (
	ErrInodeHasNotPrev         = logex.Define("the prev of inode is nil")
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
}

func NewVolume(f *flow.Flow, raw bio.RawDisker) (*Volume, error) {
	vol := &Volume{
		flow: f.Fork(0),
		raw:  raw,
	}

	if err := vol.init(); err != nil {
		return nil, logex.Trace(err)
	}

	vol.flow.AddOnClose(vol.Close)
	return vol, nil
}

func (v *Volume) init() error {
	v.inodeMgr = NewInodeMgr(v.flow)
	if err := v.inodeMgr.Init(v.raw); err != nil {
		return logex.Trace(err)
	}

	pointer := v.inodeMgr.GetPointer()
	v.dev = bio.NewDevice(v.raw, *pointer)
	v.devmgr = bio.NewDeviceMgr(v.flow, v.dev, pointer)

	v.inodeMgr.Start(v.devmgr)

	// make sure rootDir is inited
	if _, err := v.inodeMgr.GetInode(0); logex.Equal(err, ErrInodeMgrInodeNotFound) {
		inode, err := v.inodeMgr.NewInode()
		if err != nil {
			return logex.Trace(err)
		}
		if inode.Ino != 0 {
			return logex.Trace(fmt.Errorf("root directory inode is not 0"))
		}
	}
	rootDir, err := NewRootDir(v)
	if err != nil {
		return logex.Trace(err)
	}
	v.rootDir = *rootDir
	return nil
}

func (v *Volume) getInoByName(name string) int32 {
	return v.rootDir.Find(name)
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

	return NewFile(v.flow, v, ino, name)
}

func (v *Volume) Close() {
	if !v.flow.MarkExit() {
		return
	}
	// clean in writeChan
	v.flow.Close()
}

// -----------------------------------------------------------------------------
// FileDelegate

func (v *Volume) ReadDeviceAt(b []byte, off int64) (int, error) {
	return v.devmgr.ReadAt(b, off)
}

func (v *Volume) MallocWriter(n int) *bio.DeviceWriter {
	return v.devmgr.MallocWriter(n)
}

func (v *Volume) GetFlushNotify() chan struct{} {
	return v.devmgr.GetFlushNotify()
}

func (v *Volume) ForceFlush() {
	err := v.inodeMgr.Flush()
	if err != nil {
		logex.Error(err)
	}
}

func (v *Volume) PrevInode(i *Inode) (*Inode, error) {
	if i.Prev == 0 {
		return nil, ErrInodeHasNotPrev.Trace()
	}

	return v.inodeMgr.GetInodeByAddr(Address(i.Prev))
}

func (v *Volume) GetInode(ino int32) (*Inode, error) {
	return v.inodeMgr.GetInode(ino)
}

func (v *Volume) NextInode(i *Inode) (*Inode, error) {
	return v.inodeMgr.GenNextInode(i)
}

func (v *Volume) DoneFlush() {
	v.devmgr.Done()
}
