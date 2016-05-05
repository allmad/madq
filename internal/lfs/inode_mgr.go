package lfs

import (
	"io"
	"time"

	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
	"github.com/chzyer/madq/internal/util"
)

var (
	ErrAddressNotValid       = logex.Define("address is not valid")
	ErrInodeMgrNotStarted    = logex.Define("not started")
	ErrInodeMgrInodeNotFound = logex.Define("inode is not found")
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
	reservedArea *ReservedArea

	// addr => Inode
	inodeMap map[Address]*Inode
	// addr => InodeTable
	inodeTableMap map[Address]*InodeTable

	// set on started, please get via `getDev()`
	dev *bio.DeviceMgr
}

func NewInodeMgr() *InodeMgr {
	im := &InodeMgr{
		reservedArea:  NewReservedArea(),
		inodeMap:      make(map[Address]*Inode),
		inodeTableMap: make(map[Address]*InodeTable),
	}
	return im
}

func (i *InodeMgr) getDev() (*bio.DeviceMgr, error) {
	if !i.state.After(InodeMgrStateStarted) {
		return nil, ErrInodeMgrNotStarted.Trace()
	}
	return i.dev, nil
}

func (i *InodeMgr) Start(dev *bio.DeviceMgr) {
	if !i.state.Set(InodeMgrStateStarting) {
		return
	}
	i.dev = dev
	i.state.Set(InodeMgrStateStarted)
}

// must Init first,
func (i *InodeMgr) GetPointer() *int64 {
	return &i.reservedArea.Superblock.Checkpoint
}

func (i *InodeMgr) Init(raw bio.RawDisker) error {
	err := bio.ReadAt(raw, 0, i.reservedArea)
	if err != nil && logex.Equal(err, io.EOF) {
		err = nil
	}
	return err
}

func (i *InodeMgr) GetInode(ino int32) (*Inode, error) {
	l1, l2 := i.reservedArea.GetIdx(ino)
	tableAddr := i.reservedArea.IndirectInodeTable[l1]
	if !tableAddr.Valid() {
		return nil, ErrInodeMgrInodeNotFound.Trace()
	}
	it, err := i.GetInodeTable(tableAddr)
	if err != nil {
		return nil, err
	}

	inodeAddr := it.Address[l2]
	if !inodeAddr.Valid() {
		return nil, ErrInodeMgrInodeNotFound.Trace()
	}
	inode, err := i.GetInodeByAddr(inodeAddr)
	if err != nil {
		return nil, err
	}
	return inode, nil
}

func (i *InodeMgr) Flush() error {
	if err := i.dev.Flush(); err != nil {
		return logex.Trace(err)
	}

	// sync reservice area
	err := bio.WriteAt(i.dev.Raw(), 0, i.reservedArea)
	if err != nil {
		return logex.Trace(err)
	}
	return nil
}

// get inode table from cache or disk
func (i *InodeMgr) GetInodeTable(addr Address) (*InodeTable, error) {
	if !addr.Valid() {
		return nil, ErrAddressNotValid.Trace()
	}

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
	i.inodeTableMap[addr] = it
	return it, nil
}

// make sure addr is Valid()
func (i *InodeMgr) GetInodeByAddr(addr Address) (*Inode, error) {
	if !addr.Valid() {
		return nil, ErrAddressNotValid.Trace()
	}

	inode, ok := i.inodeMap[addr]
	if ok {
		return inode, nil
	}

	dev, err := i.getDev()
	if err != nil {
		return nil, logex.Trace(err)
	}
	inode = new(Inode)
	if err := bio.ReadDiskable(dev, int64(addr), inode); err != nil {
		return nil, logex.Trace(err)
	}
	i.inodeMap[addr] = inode
	return inode, nil
}

func (i *InodeMgr) InodeCount() int {
	return int(i.reservedArea.Superblock.InodeCnt)
}

func (i *InodeMgr) RemoveInode(ino int32) error {
	return i.removeInode(ino)
}

func (i *InodeMgr) removeInode(ino int32) error {
	dev, err := i.getDev()
	if err != nil {
		return logex.Trace(err)
	}

	i.reservedArea.Superblock.InodeCnt--
	l1, l2 := i.reservedArea.GetIdx(ino)
	inodeTableAddr := i.reservedArea.IndirectInodeTable[l1]
	inodeTable, err := i.GetInodeTable(inodeTableAddr)
	if err != nil {
		return logex.Trace(err)
	}

	inodeTableWriter := dev.MallocWriter(InodeTableSize)
	inodeAddr := inodeTable.Address[l2]
	inodeTable.Address[l2] = 0
	inodeTableWriter.WriteDisk(inodeTable)
	i.reservedArea.IndirectInodeTable[l1] = Address(inodeTableWriter.Offset())
	delete(i.inodeMap, inodeAddr)
	delete(i.inodeTableMap, inodeTableAddr)
	i.inodeTableMap[Address(inodeTableWriter.Offset())] = inodeTable

	inodeTableWriter.Close()

	return nil
}

func (i *InodeMgr) NewInode() (*Inode, error) {
	return i.newInode()
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
	inodeWriter := dev.MallocWriter(InodeSize)
	inodeWriter.WriteDisk(inode)
	i.inodeMap[Address(inodeWriter.Offset())] = inode

	l1, l2 := i.reservedArea.GetIdx(inode.Ino)
	addrInodeTable := i.reservedArea.IndirectInodeTable[l1]
	inodeTable, _ := i.GetInodeTable(addrInodeTable)
	if inodeTable != nil {
		inodeTable.Address[l2] = Address(inodeWriter.Offset())
		inodeTableWriter := dev.MallocWriter(InodeTableSize)
		i.reservedArea.IndirectInodeTable[l1] = Address(inodeTableWriter.Offset())
		inodeTableWriter.WriteDisk(inodeTable)
		i.inodeTableMap[Address(inodeTableWriter.Offset())] = inodeTable
		delete(i.inodeTableMap, Address(addrInodeTable))
		inodeTableWriter.Close()
	} else {
		// make a new inodeTable
		inodeTable = new(InodeTable)
		InodeTableWriter := dev.MallocWriter(InodeTableSize)
		// set the first address to offset of Inode
		inodeTable.Address[l2] = Address(inodeWriter.Offset())
		addrInodeTable.Update(Address(InodeTableWriter.Offset()))
		i.inodeTableMap[Address(InodeTableWriter.Offset())] = inodeTable
		InodeTableWriter.WriteDisk(inodeTable)
		i.reservedArea.IndirectInodeTable[l1] = Address(InodeTableWriter.Offset())
		InodeTableWriter.Close()
	}

	inodeWriter.Close()

	return inode, nil
}
