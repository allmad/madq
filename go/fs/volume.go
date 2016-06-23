package fs

import (
	"fmt"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/go/bio"
)

type FileName [24]byte

type Volume struct {
	flow      *flow.Flow
	header    *VolumeHeader
	fileCache map[string]*File
}

type VolumeDelegate interface {
	bio.ReadWriterAt
}

type VolumeConfig struct {
	Delegate VolumeDelegate
}

func NewVolume(f *flow.Flow, cfg *VolumeConfig) (*Volume, error) {
	vh, err := ReadVolumeHeader(cfg.Delegate)
	if err != nil {
		// make a new one
		return nil, err
	}
	vol := &Volume{
		header:    vh,
		fileCache: make(map[string]*File, 16),
	}
	f.ForkTo(&vol.flow, vol.Close)
	return vol, nil
}

func (v *Volume) getFileInCache(name string) *File {
	return v.fileCache[name]
}

func (v *Volume) Open(name string) (*File, error) {
	if len(name) > 24 {
		return nil, fmt.Errorf("filename exceed 24bytes")
	}

	if fd := v.getFileInCache(name); fd != nil {
		return fd, nil
	}

	fd, err := NewFile(v.flow, &FileConfig{})
	if err != nil {
		return nil, err
	}

	return fd, nil
}

func (v *Volume) Close() {

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
	if err := WriteDisk(rw, vh, 0); err != nil {
		return nil, logex.Trace(err)
	}

}

func ReadVolumeHeader(rw bio.ReadWriterAt) (*VolumeHeader, error) {
	vh := new(VolumeHeader)
	if err := ReadDisk(rw, vh, 0); err != nil {
		return nil, logex.Trace(err)
	}
	if vh.Checkpoint < MinVolumeHeaderCheckpoint {
		return nil, logex.NewError("invalid checkpoint:", vh.Checkpoint)
	}

	imap, err := NewInodeMap(VolumeHeaderSize, rw)
	if err != nil {
		return nil, err
	}
	vh.InodeMap = imap
	return vh, nil
}
