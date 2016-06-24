package fs

import (
	"io"
	"os"
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/go/bio"
)

const FileNameSize = 28

var ErrFileNotExist = logex.Define("file is not exists")

type FileName [FileNameSize]byte

type Volume struct {
	cfg       *VolumeConfig
	flow      *flow.Flow
	header    *VolumeHeader
	delegate  VolumeDelegate
	fileCache map[string]*File

	// init
	flusher *Flusher
	nameMap *NameMap
}

type VolumeDelegate interface {
	bio.ReadWriterAt
	ReadData(off int64, n int) ([]byte, error)
}

type VolumeConfig struct {
	Delegate      VolumeDelegate
	FlushInterval time.Duration
	FlushSize     int
}

func NewVolume(f *flow.Flow, cfg *VolumeConfig) (*Volume, error) {
	vh, err := ReadVolumeHeader(cfg.Delegate)
	if err != nil {
		if !logex.Equal(err, io.EOF) {
			return nil, logex.Trace(err)
		}

		// make a new one
		vh, err = GenNewVolumeHeader(cfg.Delegate)
		if err != nil {
			return nil, logex.Trace(err)
		}
	}

	vol := &Volume{
		cfg:       cfg,
		header:    vh,
		delegate:  cfg.Delegate,
		fileCache: make(map[string]*File, 16),
	}
	f.ForkTo(&vol.flow, vol.Close)

	if err := vol.init(); err != nil {
		return nil, err
	}
	return vol, nil
}

func (v *Volume) init() (err error) {
	v.flusher = v.initFlusher()
	v.nameMap, err = v.initNameMap()
	if err != nil {
		return err
	}
	return nil
}

func (v *Volume) initFlusher() *Flusher {
	return NewFlusher(v.flow, &FlusherConfig{
		Offset:   int64(v.header.Checkpoint),
		Interval: v.cfg.FlushInterval,
		Delegate: v.delegate,
	})
}

func (v *Volume) initNameMap() (*NameMap, error) {
	fd, err := v.InoOpen(0, os.O_CREATE)
	if err != nil {
		return nil, logex.Trace(err)
	}
	return NewNameMap(fd)
}

func (v *Volume) getFileInCache(name string) *File {
	return v.fileCache[name]
}

func (v *Volume) InoOpen(ino int32, flags int) (*File, error) {
	fd, err := NewFile(v.flow, &FileConfig{
		Ino:           ino,
		Flags:         flags,
		Delegate:      &volumeFileDelegate{v.delegate, v.header.InodeMap},
		FlushInterval: v.cfg.FlushInterval,
		FlushSize:     v.cfg.FlushSize,
		Flusher:       v.flusher,
	})
	return fd, err
}

func (v *Volume) Open(name string, flags int) (*File, error) {
	if fd := v.getFileInCache(name); fd != nil {
		return fd, nil
	}

	ino, err := v.nameMap.GetIno(name)
	if err != nil {
		return nil, logex.Trace(err)
	}

	if ino < 0 && !IsFileCreate(flags) {
		return nil, ErrFileNotExist.Trace()
	}
	if ino < 0 {
		// alloc ino
		ino, err = v.nameMap.GetFreeIno()
		if err != nil {
			return nil, err
		}
		if err := v.nameMap.AddIno(name, ino); err != nil {
			return nil, err
		}
	}

	fd, err := v.InoOpen(ino, flags)
	if err != nil {
		return nil, logex.Trace(err)
	}
	return fd, nil
}

func (v *Volume) Close() {
	v.flusher.Close()
	v.nameMap.Close()
	v.flow.Close()
}

// -----------------------------------------------------------------------------
var _ FileDelegater = new(volumeFileDelegate)

type volumeFileDelegate struct {
	v    VolumeDelegate
	imap *InodeMap
}

func (v *volumeFileDelegate) GetInode(ino int32) (*Inode, error) {
	return v.imap.GetInode(ino)
}

func (v *volumeFileDelegate) GetInodeByAddr(addr Address) (*Inode, error) {
	return v.imap.GetInodeByAddr(addr)
}

func (v *volumeFileDelegate) ReadData(addr ShortAddr, n int) ([]byte, error) {
	return v.v.ReadData(int64(addr), n)
}

func (v *volumeFileDelegate) SaveInode(ino *Inode) {
	v.imap.SaveInode(ino)
}

// -----------------------------------------------------------------------------
const VolumeHeaderSize = 16
const MinVolumeHeaderCheckpoint = VolumeHeaderSize + InodeMapSize

type VolumeHeader struct {
	Version    Int32
	Checkpoint Address
	InodeMap   *InodeMap
}

func (v *VolumeHeader) DiskSize() int {
	return VolumeHeaderSize
}

func (v *VolumeHeader) WriteDisk(b []byte) {
	dw := NewDiskWriter(b)
	dw.WriteMagic(v)
	dw.WriteItem(v.Version)
	dw.WriteItem(v.Checkpoint)
}

func (v *VolumeHeader) ReadDisk(b []byte) error {
	dr := NewDiskReader(b)
	if err := dr.ReadMagic(v); err != nil {
		return err
	}
	if err := dr.ReadItem(&v.Version); err != nil {
		return err
	}
	if err := dr.ReadItem(&v.Checkpoint); err != nil {
		return err
	}
	return nil
}

func (v *VolumeHeader) Magic() Magic {
	return MagicVolume
}

func GenNewVolumeHeader(rw bio.ReadWriterAt) (*VolumeHeader, error) {
	vh := new(VolumeHeader)
	vh.Version = 1
	vh.Checkpoint = MinVolumeHeaderCheckpoint
	if err := WriteDiskAt(rw, vh, 0); err != nil {
		return nil, logex.Trace(err)
	}
	imap, err := NewInodeMap(VolumeHeaderSize, rw, true)
	if err != nil {
		return nil, err
	}
	vh.InodeMap = imap
	return vh, nil
}

func ReadVolumeHeader(rw bio.ReadWriterAt) (*VolumeHeader, error) {
	vh := new(VolumeHeader)
	if err := ReadDisk(rw, vh, 0); err != nil {
		return nil, logex.Trace(err)
	}
	if vh.Checkpoint < MinVolumeHeaderCheckpoint {
		return nil, logex.NewError("invalid checkpoint:", vh.Checkpoint)
	}

	imap, err := NewInodeMap(VolumeHeaderSize, rw, false)
	if err != nil {
		return nil, err
	}
	vh.InodeMap = imap
	return vh, nil
}
