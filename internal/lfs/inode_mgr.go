package lfs

import (
	"io"
	"sync"
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/madq/internal/bio"
	"github.com/chzyer/madq/internal/util"
)

var (
	ErrAddressIsInMemory     = logex.Define("address is in memory")
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
	flow         *flow.Flow
	state        InodeMgrState
	reservedArea *ReservedArea
	needFlush    chan struct{}

	// addr => Inode
	inodeMap map[Address]*Inode
	// addr => InodeTable
	inodeTableMap map[Address]*InodeTable

	dirtyGuard      sync.Mutex
	dirtyInode      []*Inode
	dirtyInodeTable []*InodeTable

	// set on started, please get via `getDev()`
	dev *bio.DeviceMgr
}

func NewInodeMgr(f *flow.Flow) *InodeMgr {
	im := &InodeMgr{
		needFlush:     make(chan struct{}, 1),
		reservedArea:  NewReservedArea(),
		inodeMap:      make(map[Address]*Inode),
		inodeTableMap: make(map[Address]*InodeTable),
	}

	f.ForkTo(&im.flow, im.Close)
	return im
}

func (i *InodeMgr) markDirtyInode(inode *Inode) {
	i.dirtyGuard.Lock()
	i.dirtyInode = append(i.dirtyInode, inode)
	i.dirtyGuard.Unlock()
	select {
	case <-i.needFlush:
	default:
	}
}

func (i *InodeMgr) markDirtyInodeTable(inodeTable *InodeTable) {
	i.dirtyGuard.Lock()
	i.dirtyInodeTable = append(i.dirtyInodeTable, inodeTable)
	i.dirtyGuard.Unlock()
	select {
	case <-i.needFlush:
	default:
	}
}

func (i *InodeMgr) loop(dev *bio.DeviceMgr) {
	i.flow.Add(1)
	defer i.flow.DoneAndClose()

	var flushChan chan struct{}
	flush := func() {
		i.dirtyGuard.Lock()
		size := len(i.dirtyInode)*InodeSize + len(i.dirtyInodeTable)*InodeTableSize

		if size > 0 && flushChan == nil {
			flushChan = dev.GetFlushNotify()
		}

		w := dev.MallocWriter(size)
		_ = w
		logex.Info(i.dirtyInode)
		for _, inode := range i.dirtyInode {
			_ = inode

		}
		i.dirtyInode = i.dirtyInode[:0]
		i.dirtyInodeTable = i.dirtyInodeTable[:0]

		i.dirtyGuard.Unlock()

		if flushChan != nil {
			dev.Done()
		}
	}

loop:
	for {
		select {
		case <-i.needFlush:
			if flushChan == nil {
				flushChan = dev.GetFlushNotify()
			}
		case <-flushChan:
			flush()
			flushChan = nil
		case <-i.flow.IsClose():
			flush()
			break loop
		}
	}
}

func (i *InodeMgr) Close() {
	i.flow.Close()
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
	go i.loop(dev)
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

func (i *InodeMgr) GenNextInode(inode *Inode) (*Inode, error) {
	oldInodeAddr, err := i.GetInodeAddr(inode.Ino)
	if err != nil {
		return nil, logex.Trace(err)
	}
	nextInode, err := i.newInode(inode, int64(oldInodeAddr))
	if err != nil {
		return nil, logex.Trace(err)
	}
	return nextInode, nil
}

func (i *InodeMgr) GetInodeAddr(ino int32) (Address, error) {
	l1, l2 := i.reservedArea.GetIdx(ino)
	tableAddr := i.reservedArea.IndirectInodeTable[l1]
	if !tableAddr.Valid() {
		return 0, ErrInodeMgrInodeNotFound.Trace()
	}

	it, err := i.GetInodeTable(tableAddr)
	if err != nil {
		return 0, err
	}

	return it.Address[l2], nil
}

func (i *InodeMgr) GetInode(ino int32) (*Inode, error) {
	inodeAddr, err := i.GetInodeAddr(ino)
	if err != nil {
		return nil, logex.Trace(err)
	}
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

	if addr.InMemory() {
		// must be found in inodeTableMap
		return nil, ErrAddressIsInMemory.Trace()
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

	return nil
}

func (i *InodeMgr) NewInode() (*Inode, error) {
	return i.newInode(nil, 0)
}

// alloc a new inode
func (i *InodeMgr) newInode(prev *Inode, addr int64) (*Inode, error) {
	var inode *Inode
	if prev == nil {
		ino := i.reservedArea.Superblock.InodeCnt
		i.reservedArea.Superblock.InodeCnt++
		inode = &Inode{
			Ino:    ino,
			Create: time.Now().UnixNano(),
		}
	} else {
		inode = &Inode{
			Ino:    prev.Ino,
			Start:  prev.End,
			End:    prev.End,
			Create: time.Now().UnixNano(),
			Prev:   addr,
		}
	}

	inodeAddr := InodeAddress(inode)
	i.inodeMap[inodeAddr] = inode
	i.markDirtyInode(inode)

	l1, l2 := i.reservedArea.GetIdx(inode.Ino)
	inodeTableAddrIndirect := i.reservedArea.IndirectInodeTable[l1]
	inodeTable, _ := i.GetInodeTable(inodeTableAddrIndirect)
	if inodeTable != nil {
		inodeTable.Address[l2] = inodeAddr
		inodeTableAddr := InodeTableAddress(inodeTable)
		i.reservedArea.IndirectInodeTable[l1] = inodeTableAddr
		if inodeTableAddr != inodeTableAddrIndirect {
			i.inodeTableMap[inodeTableAddr] = inodeTable
			delete(i.inodeTableMap, inodeTableAddrIndirect)
		}
	} else {
		// make a new inodeTable
		inodeTable = new(InodeTable)
		// set the first address to offset of Inode
		inodeTable.Address[l2] = inodeAddr
		inodeTableAddr := InodeTableAddress(inodeTable)
		inodeTableAddrIndirect.Update(inodeTableAddr)
		i.inodeTableMap[inodeTableAddr] = inodeTable
		i.reservedArea.IndirectInodeTable[l1] = inodeTableAddr
	}
	i.markDirtyInodeTable(inodeTable)

	return inode, nil
}
