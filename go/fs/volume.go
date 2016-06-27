package fs

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/go/bio"
)

var ErrFileNotExist = logex.Define("file is not exists")

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

func (v *VolumeConfig) init() error {
	if v.Delegate == nil {
		return fmt.Errorf("volume init error: delegate is empty")
	}
	if v.FlushInterval == 0 {
		v.FlushInterval = time.Second
	}
	if v.FlushSize == 0 {
		v.FlushSize = 10 << 20
	}
	return nil
}

func NewVolume(f *flow.Flow, cfg *VolumeConfig) (*Volume, error) {
	if err := cfg.init(); err != nil {
		return nil, err
	}

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

func (v *Volume) FlushInodeMap() error {
	return v.header.InodeMap.Flush()
}

// require header is inited
func (v *Volume) initFlusher() *Flusher {
	f := NewFlusher(v.flow, &FlusherConfig{
		Offset:   int64(v.header.Checkpoint),
		Interval: v.cfg.FlushInterval,
		Delegate: &volumeFlusherDelegate{v.header, v.delegate},
	})

	return f
}

func (v *Volume) initNameMap() (*NameMap, error) {
	fd, err := v.inoOpen(0, "/", os.O_CREATE)
	if err != nil {
		return nil, logex.Trace(err)
	}
	return NewNameMap(NewHandle(fd, 0), 1)
}

func (v *Volume) addCache(fd *File) {
	v.fileCache[fd.Name()] = fd
}

func (v *Volume) getFileInCache(name string) *Handle {
	f := v.fileCache[name]
	if f != nil {
		if !f.AddRef() {
			delete(v.fileCache, name)
			f = nil
			return nil
		}
		return NewHandle(f, 0)
	}
	return nil
}

func (v *Volume) inoOpen(ino int32, name string, flags int) (*File, error) {
	fd, err := NewFile(v.flow, &FileConfig{
		Ino:           ino,
		Flags:         flags,
		Name:          name,
		Delegate:      &volumeFileDelegate{v.delegate, v.header.InodeMap},
		FlushInterval: v.cfg.FlushInterval,
		FlushSize:     v.cfg.FlushSize,
		Flusher:       v.flusher,
	})
	if err != nil {
		return nil, err
	}
	v.addCache(fd)
	return fd, nil
}

func (v *Volume) Open(name string, flags int) (*Handle, error) {
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

	fd, err := v.inoOpen(ino, name, flags)
	if err != nil {
		return nil, logex.Trace(err)
	}
	if !fd.AddRef() {
		panic("can't be fail")
	}

	// add cache
	return NewHandle(fd, 0), nil
}

func (v *Volume) List() []string {
	return v.nameMap.List()
}

func (v *Volume) CleanCache() {
	v.fileCache = make(map[string]*File)
}

func (v *Volume) Close() {
	v.flusher.Close()
	v.nameMap.Close()
	v.flow.Close()
	if err := v.header.Flush(v.delegate); err != nil {
		println("volume header flush error:", err.Error())
	}
}

// -----------------------------------------------------------------------------

var _ FlushDelegate = new(volumeFlusherDelegate)

type volumeFlusherDelegate struct {
	header *VolumeHeader
	VolumeDelegate
}

func (v *volumeFlusherDelegate) UpdateCheckpoint(cp int64) {
	v.header.Checkpoint.Set(Address(cp))
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

const (
	VolumeHeaderSize          = 16
	VolumeHeaderMinCheckpoint = VolumeHeaderSize + InodeMapSize
)

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

func (v *VolumeHeader) Flush(w io.WriterAt) error {
	if err := WriteDiskAt(w, v, 0); err != nil {
		return err
	}
	if err := v.InodeMap.Flush(); err != nil {
		return err
	}
	return nil
}

func GenNewVolumeHeader(rw bio.ReadWriterAt) (*VolumeHeader, error) {
	vh := new(VolumeHeader)
	vh.Version = 1
	vh.Checkpoint = VolumeHeaderMinCheckpoint
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
	if vh.Checkpoint < VolumeHeaderMinCheckpoint {
		return nil, logex.NewError("invalid checkpoint:", vh.Checkpoint)
	}

	imap, err := NewInodeMap(VolumeHeaderSize, rw, false)
	if err != nil {
		return nil, err
	}
	vh.InodeMap = imap
	return vh, nil
}
