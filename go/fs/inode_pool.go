package fs

import (
	"fmt"
	"unsafe"

	"github.com/chzyer/logex"
)

type InodePoolDelegate interface {
	GetInode(ino int32) (*Inode, error)
	GetInodeByAddr(addr Address) (*Inode, error)
	SaveInode(inode *Inode)
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
	nextInode   map[Address]*Inode
}

func NewInodePool(ino int32, delegate InodePoolDelegate) *InodePool {
	p := &InodePool{
		ino:      ino,
		delegate: delegate,
	}
	p.ResetCache()
	return p
}

func (p *InodePool) ResetCache() {
	p.pool = make(map[Address]*Inode, 32)
	p.offsetInode = make(map[int32]*Inode, 32)
	p.nextInode = make(map[Address]*Inode, 32)
}

func (p *InodePool) SeekNext(inode *Inode) (*Inode, error) {
	if next := p.getNextInCache(inode); next != nil {
		return next, nil
	}
	return p.seekPrev(int64(inode.Start*BlockSize + InodeCap))
}

func (p *InodePool) SeekPrev(offset int64) (*Inode, error) {
	return p.seekPrev(offset)
}

func (p *InodePool) getInoIdxInCache(inoIdx int32) *Inode {
	ino := p.offsetInode[inoIdx]
	Stat.Inode.Cache.InoIdxHit.HitIf(ino != nil)
	return ino
}

func (p *InodePool) seekPrev(offset int64) (*Inode, error) {
	inoIdx := GetInodeIdx(offset)
	if inode := p.getInoIdxInCache(inoIdx); inode != nil {
		return inode, nil
	}

	lastest, err := p.getInScatter(0)
	if err != nil {
		return nil, logex.Trace(err)
	}

	ino, err := p.seekInode(lastest, inoIdx)
	if err != nil {
		return nil, logex.Trace(err)
	}
	return ino, nil
}

func (p *InodePool) getPrevInode(ino *Inode, n int32) (*Inode, error) {
	if n == 0 {
		return ino, nil
	}
	if n < 0 {
		panic(fmt.Sprint("distance < 0: ", n))
	}

	addr := *ino.PrevInode[inodeOffsetIdx[n]]
	if ino := p.pool[addr]; ino != nil {
		return ino, nil
	}
	if addr.IsInMem() {
		panic("not found in memory")
	}
	if addr.IsEmpty() {
		panic("addr is empty")
	}

	Stat.Inode.ReadDisk.Add(1)
	ino, err := p.delegate.GetInodeByAddr(addr)
	if err != nil {
		return nil, err
	}
	p.addCache(ino)

	return ino, nil
}

func (p *InodePool) seekInode(base *Inode, inoIdx int32) (*Inode, error) {
	start := int32(base.Start / InodeBlockCnt)
	distance := inoIdx - start
	if distance == 0 {
		return base, nil
	}

	// TODO: try to found in scatter

	for tryTime := 0; ; tryTime++ {
		newIno, err := p.getPrevInode(base, distance)
		if err != nil {
			return nil, err
		}
		if int32(newIno.Start) == inoIdx {
			Stat.Inode.PrevSeekCnt.HitN(tryTime)
			return newIno, nil
		}
		base = newIno
	}

}

func (p *InodePool) CleanCache() {
	p.pool = make(map[Address]*Inode, 32)
	p.scatter.Clean()
}

func (p *InodePool) RefPayloadBlock() (*Inode, int, error) {
	inode, err := p.GetLastest()
	if err != nil {
		return nil, -1, logex.Trace(err)
	}
	idx := inode.GetSizeIdx()
	if idx <= len(inode.Offsets)-1 {
		return inode, idx, nil
	}

	// old inode is full
	return p.next(inode), 0, nil
}

func (p *InodePool) getInPool(addr Address) *Inode {
	return p.pool[addr]
}

func (p *InodePool) setPrevs(inode *Inode) error {
	pair := []int{0, 1, 3, 7, 15, 31}
	for idx, sIdx := range pair {
		ino, err := p.getInScatter(sIdx)
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

func (p *InodePool) OnFlush(ino *Inode, addr Address) {
	if ino.addr == addr {
		return
	}
	p.updateInodeAddr(ino, addr)
	lastest := p.scatter.Top()
	if ino == nil {
		panic("top os scatter is nil")
	}
	if lastest == ino {
		p.delegate.SaveInode(ino)
	}
}

func (p *InodePool) updateInodeAddr(ino *Inode, addr Address) {
	oldAddr := ino.addr
	ino.addr.Set(addr)
	p.addCache(ino)
	delete(p.pool, oldAddr)
}

func (p *InodePool) InitInode() *Inode {
	ret := NewInode(p.ino)
	ret.addr.SetMem(unsafe.Pointer(ret))
	p.addCache(ret)
	p.scatter.Push(ret)
	return ret
}

func (p *InodePool) next(lastest *Inode) *Inode {
	ret := &Inode{
		Ino:       lastest.Ino,
		Start:     lastest.Start + Int32(len(lastest.Offsets)),
		PrevInode: emptyPrevs,
	}
	p.setPrevs(ret)

	ret.addr.SetMem(unsafe.Pointer(ret))
	p.addCache(ret)

	p.scatter.Push(ret)
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

func (p *InodePool) getNextInCache(i *Inode) *Inode {
	ino := p.nextInode[i.addr]
	Stat.Inode.Cache.NextHit.HitIf(ino != nil)
	return ino
}

func (p *InodePool) addCache(i *Inode) {
	p.pool[i.addr] = i
	p.offsetInode[int32(i.Start/InodeBlockCnt)] = i
	p.nextInode[*i.PrevInode[0]] = i
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

func (is *InodeScatter) Top() *Inode {
	return (*is)[len(*is)-1]
}

func (is *InodeScatter) Push(i *Inode) {
	copy(is[:], is[1:])
	is[len(is)-1] = i
}
