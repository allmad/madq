package fs

import (
	"fmt"
	"unsafe"

	"github.com/chzyer/logex"
)

type InodePoolDelegate interface {
	GetInode(ino int32) (*Inode, error)
	GetInodeByAddr(addr Address) (*Inode, error)
}

var inodeOffsetIdx = initOffsetIdx()

// Pool for one file
type InodePool struct {
	ino int32

	scatter InodeScatter

	delegate InodePoolDelegate
	pool     map[Address]*Inode

	// cache for seek
	offsetInode map[int32]*Inode // inoIdx => Inode
}

func NewInodePool(ino int32, delegate InodePoolDelegate) *InodePool {
	return &InodePool{
		ino:         ino,
		delegate:    delegate,
		pool:        make(map[Address]*Inode, 32),
		offsetInode: make(map[int32]*Inode, 32),
	}
}

func (i *InodePool) SeekNext(inode *Inode) (*Inode, error) {
	return i.seekPrev(int64(inode.Start*BlockSize + InodeCap))
}

func (i *InodePool) SeekPrev(offset int64) (*Inode, error) {
	return i.seekPrev(offset)
}

func (i *InodePool) seekPrev(offset int64) (*Inode, error) {
	inoIdx := GetInodeIdx(offset)
	if inode := i.offsetInode[inoIdx]; inode != nil {
		return inode, nil
	}

	lastest, err := i.getInScatter(0)
	if err != nil {
		return nil, logex.Trace(err)
	}

	ino, err := i.seekInode(lastest, inoIdx)
	if err != nil {
		return nil, logex.Trace(err)
	}
	return ino, nil
}

func (i *InodePool) getPrevInode(ino *Inode, n int32) (*Inode, error) {
	if n == 0 {
		return ino, nil
	}
	if n < 0 {
		panic(fmt.Sprint("distance < 0: ", n))
	}

	addr := *ino.PrevInode[inodeOffsetIdx[n]]
	if ino := i.pool[addr]; ino != nil {
		return ino, nil
	}
	if addr.IsInMem() {
		panic("not found in memory")
	}
	if addr.IsEmpty() {
		panic("addr is empty")
	}

	ino, err := i.delegate.GetInodeByAddr(addr)
	if err != nil {
		return nil, err
	}
	i.addCache(ino)

	return ino, nil
}

func (i *InodePool) seekInode(base *Inode, inoIdx int32) (*Inode, error) {
	start := int32(base.Start / InodeBlockCnt)
	distance := inoIdx - start
	if distance == 0 {
		return base, nil
	}

	// TODO: try to found in scatter

	for {
		newIno, err := i.getPrevInode(base, distance)
		if err != nil {
			return nil, err
		}
		if int32(newIno.Start) == inoIdx {
			return newIno, nil
		}
		base = newIno
	}
}

func (i *InodePool) CleanCache() {
	i.pool = make(map[Address]*Inode, 32)
	i.scatter.Clean()
}

func (i *InodePool) RefPayloadBlock() (*Inode, int, error) {
	inode, err := i.GetLastest()
	if err != nil {
		return nil, -1, logex.Trace(err)
	}
	idx := inode.GetSizeIdx()
	if idx <= len(inode.Offsets)-1 {
		return inode, idx, nil
	}

	// old inode is full
	return i.next(inode), 0, nil
}

func (i *InodePool) getInPool(addr Address) *Inode {
	return i.pool[addr]
}

func (i *InodePool) setPrevs(inode *Inode) error {
	pair := []int{0, 1, 3, 7, 15, 31}
	for idx, sIdx := range pair {
		ino, err := i.getInScatter(sIdx)
		if err != nil {
			return err
		}
		if ino == nil {
			break
		}
		inode.PrevInode[idx] = &ino.addr
	}
	return nil
}

func (i *InodePool) OnFlush(ino *Inode, addr Address) {
	if ino.addr == addr {
		return
	}
	oldAddr := ino.addr
	ino.addr.Set(addr)
	i.addCache(ino)
	delete(i.pool, oldAddr)
}

func (i *InodePool) InitInode() *Inode {
	ret := NewInode(i.ino)
	ret.addr.SetMem(unsafe.Pointer(ret))
	i.addCache(ret)
	i.scatter.Push(ret)
	return ret
}

func (i *InodePool) next(lastest *Inode) *Inode {
	ret := &Inode{
		Ino:       lastest.Ino,
		Start:     lastest.Start + Int32(len(lastest.Offsets)),
		PrevInode: emptyPrevs,
	}
	i.setPrevs(ret)

	ret.addr.SetMem(unsafe.Pointer(ret))
	i.addCache(ret)

	i.scatter.Push(ret)
	return ret
}

// get newest inode from memory or disk
func (i *InodePool) GetLastest() (*Inode, error) {
	return i.getInScatter(0)
}

func (i *InodePool) GetByAddr(addr Address) (*Inode, error) {
	ino, ok := i.pool[addr]
	if !ok {
		var err error
		ino, err = i.delegate.GetInodeByAddr(addr)
		if err != nil {
			return nil, logex.Trace(err)
		}
		i.addCache(ino)
	}
	return ino, nil
}

func (p *InodePool) addCache(i *Inode) {
	p.pool[i.addr] = i
	p.offsetInode[int32(i.Start/InodeBlockCnt)] = i
}

// try to load inode one by one into memory
// if return (nil, nil), means this file not have so much inodes yet.
func (i *InodePool) getInScatter(n int) (*Inode, error) {
	idx := len(i.scatter) - 1 - n
	if i.scatter[idx] != nil {
		return i.scatter[idx], nil
	}

	k := len(i.scatter) - 1
	for ; k <= len(i.scatter)-1-n; k-- {
		if i.scatter[k] == nil {
			// lastest one
			if k == len(i.scatter)-1 {
				inode, err := i.delegate.GetInode(i.ino)
				if err != nil {
					return nil, logex.Trace(err, k)
				}
				i.scatter[k] = inode
			} else {
				if i.scatter[k+1].Start == 0 {
					break
				}
				addr := i.scatter[k+1].PrevInode[0]
				if addr.IsInMem() {
					panic("can't be in mem")
				}
				inode, err := i.delegate.GetInodeByAddr(*addr)
				if err != nil {
					return nil, logex.Trace(err, k)
				}
				i.scatter[k] = inode
			}
			// already the first one
			if i.scatter[k].Start == 0 {
				break
			}
		}
	}

	return i.scatter[len(i.scatter)-1-n], nil
}

type InodeScatter [32]*Inode

func (is *InodeScatter) Clean() {
	for idx := range is {
		is[idx] = nil
	}
}

func (is *InodeScatter) Push(i *Inode) {
	copy(is[:], is[1:])
	is[len(is)-1] = i
}
