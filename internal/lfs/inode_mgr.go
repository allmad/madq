package lfs

import (
	"io"

	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
	"github.com/chzyer/madq/internal/util"
)

var (
	ErrInodeMgrNotStarted = logex.Define("not started")
)

type InodeMgrState int32

const (
	InodeMgrStateInit InodeMgrState = iota
	InodeMgrStateStarting
	InodeMgrStateStarted
	InodeMgrStateClosed
)

func (i *InodeMgrState) Set(val InodeMgrState) bool {
	return ((*util.State)(i)).Set(util.State(val))
}

func (i *InodeMgrState) After(val InodeMgrState) bool {
	return ((*util.State)(i)).After(util.State(val))
}

type InodeMgr struct {
	state        InodeMgrState
	raw          bio.RawDisker
	reservedArea ReservedArea

	// addr => Inode
	inodeMap map[Address]*Inode
	// addr => InodeTable
	inodeTableMap map[Address]*InodeTable

	// set on started, please get via `getDev()`
	dev *bio.Device
}

func NewInodeMgr(raw bio.RawDisker) *InodeMgr {
	im := &InodeMgr{
		raw:           raw,
		inodeMap:      make(map[Address]*Inode),
		inodeTableMap: make(map[Address]*InodeTable),
	}
	return im
}

func (i *InodeMgr) getDev() (*bio.Device, error) {
	if !i.state.After(InodeMgrStateStarted) {
		return nil, ErrInodeMgrNotStarted.Trace()
	}
	return i.dev, nil
}

func (i *InodeMgr) Start(dev *bio.Device) {
	if !i.state.Set(InodeMgrStateStarting) {
		return
	}
	defer i.state.Set(InodeMgrStateStarted)
	i.dev = dev
}

func (i *InodeMgr) init() error {
	err := bio.ReadAt(i.raw, 0, &i.reservedArea)
	if err != nil && logex.Equal(err, io.EOF) {
		err = logex.Trace(bio.WriteAt(i.raw, 0, &i.reservedArea))
	}
	return err
}

func (i *InodeMgr) GetInode(ino int) (*Inode, error) {
	l1, l2 := i.reservedArea.GetIdx(ino)
	it, err := i.GetInodeTable(i.reservedArea.IndirectInodeTable[l1])
	if err != nil {
		return nil, err
	}
	inode, err := i.GetInodeByAddr(it.Address[l2])
	if err != nil {
		return nil, err
	}
	return inode, nil
}

func (i *InodeMgr) GetInodeTable(addr Address) (*InodeTable, error) {
	it, ok := i.inodeTableMap[addr]
	if ok {
		return it, nil
	}

	// read from disk
	it = new(InodeTable)
	if err := bio.ReadDiskable(i.raw, int64(addr), it); err != nil {
		return nil, logex.Trace(err)
	}
	return it, nil
}

// make sure addr is Valid()
func (im *InodeMgr) GetInodeByAddr(addr Address) (*Inode, error) {
	i, ok := im.inodeMap[addr]
	if ok {
		return i, nil
	}

	inode := new(Inode)
	if err := bio.ReadDiskable(im.raw, int64(addr), inode); err != nil {
		return nil, logex.Trace(err)
	}
	return inode, nil
}

func (i *InodeMgr) newInode() (*Inode, error) {
	dev, err := i.getDev()
	if err != nil {
		return nil, logex.Trace(err)
	}
	_ = dev
	inode := new(Inode)
	size := i.reservedArea.Superblock.InodeCnt
	i.reservedArea.Superblock.InodeCnt++
	l1, l2 := i.reservedArea.GetIdx(int(size))
	_ = l2
	addrL1 := i.reservedArea.IndirectInodeTable[l1]
	if !addrL1.Valid() {
		// make a new inodeTable

	}
	return inode, nil
}
