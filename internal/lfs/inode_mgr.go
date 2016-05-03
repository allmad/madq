package lfs

import (
	"io"
	"time"

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

type InodeMgrDelegate interface {
	Malloc(n int) int64
}

type InodeMgr struct {
	state        InodeMgrState
	delegate     InodeMgrDelegate
	reservedArea ReservedArea

	// addr => Inode
	inodeMap map[Address]*Inode
	// addr => InodeTable
	inodeTableMap map[Address]*InodeTable

	// set on started, please get via `getDev()`
	dev *bio.Device
}

func NewInodeMgr(delegate InodeMgrDelegate) *InodeMgr {
	im := &InodeMgr{
		delegate:      delegate,
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
	i.dev = dev
	i.state.Set(InodeMgrStateStarted)
}

// must Init first,
func (i *InodeMgr) GetPointer() int64 {
	return i.reservedArea.Superblock.Checkpoint
}

func (i *InodeMgr) Init(raw bio.RawDisker) error {
	err := bio.ReadAt(raw, 0, &i.reservedArea)
	if err != nil && logex.Equal(err, io.EOF) {
		err = logex.Trace(bio.WriteAt(raw, 0, &i.reservedArea))
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

	dev, err := i.getDev()
	if err != nil {
		return nil, err
	}
	// read from disk
	it = new(InodeTable)
	if err := bio.ReadDiskable(dev, int64(addr), it); err != nil {
		return nil, logex.Trace(err)
	}
	return it, nil
}

// make sure addr is Valid()
func (i *InodeMgr) GetInodeByAddr(addr Address) (*Inode, error) {
	inode, ok := i.inodeMap[addr]
	if ok {
		return inode, nil
	}

	dev, err := i.getDev()
	if err != nil {
		return nil, err
	}
	inode = new(Inode)
	if err := bio.ReadDiskable(dev, int64(addr), inode); err != nil {
		return nil, logex.Trace(err)
	}
	return inode, nil
}

// alloc a new inode
func (i *InodeMgr) newInode() (*Inode, error) {
	dev, err := i.getDev()
	if err != nil {
		return nil, logex.Trace(err)
	}

	size := i.reservedArea.Superblock.InodeCnt
	i.reservedArea.Superblock.InodeCnt++

	inode := &Inode{
		Ino:    size,
		Start:  0,
		End:    0,
		Create: time.Now().UnixNano(),
	}
	offInode := i.delegate.Malloc(InodeSize)
	inode.WriteDisk(dev.GetWriter(offInode, InodeSize))
	i.inodeMap[Address(offInode)] = inode

	l1, l2 := i.reservedArea.GetIdx(int(inode.Ino))
	_ = l2
	addrInodeTable := i.reservedArea.IndirectInodeTable[l1]
	inodeTable, _ := i.GetInodeTable(addrInodeTable)
	if inodeTable == nil {
		// make a new inodeTable
		inodeTable = new(InodeTable)
		off := i.delegate.Malloc(InodeTableSize)
		// set the first address to offset of Inode
		inodeTable.Address[0] = Address(offInode)
		addrInodeTable.Set(Address(off))
		inodeTable.WriteDisk(dev.GetWriter(off, InodeTableSize))
	} else {
		// inode table is already exists
		available := inodeTable.FindAvailable()
		if available == -1 {
			panic("could not happend")
		}
		inodeTable.Address[available] = Address(offInode)
		// write to disk
		// update indirect inode table
		// delete in map
	}

	return inode, nil
}
