package fs

import (
	"unsafe"

	"github.com/chzyer/logex"
)

type InodePoolDelegate interface {
	GetInode(ino int32) (*Inode, error)
	GetInodeByAddr(addr Address) (*Inode, error)
}

// Pool for one file
type InodePool struct {
	ino int32

	scatter InodeScatter

	delegate InodePoolDelegate
	pool     map[Address]*Inode
}

func NewInodePool(ino int32, delegate InodePoolDelegate) *InodePool {
	return &InodePool{
		ino:      ino,
		delegate: delegate,
		pool:     make(map[Address]*Inode, 32),
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
	idx := inode.GetOffsetIdx()
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
	pair := []struct {
		addr **Address
		idx  int
	}{
		{&inode.Prev, 0},
		{&inode.Prev2, 1},
		{&inode.Prev4, 3},
		{&inode.Prev8, 7},
		{&inode.Prev16, 15},
		{&inode.Prev32, 31},
	}
	for _, p := range pair {
		ino, err := i.getInScatter(p.idx)
		if err != nil {
			return err
		}
		if ino == nil {
			break
		}
		*p.addr = &ino.addr
	}
	return nil
}

func (i *InodePool) OnFlush(ino *Inode, addr Address) {
	if ino.addr == addr {
		return
	}
	i.pool[addr] = ino
	oldAddr := ino.addr
	ino.addr.Set(addr)
	delete(i.pool, oldAddr)
}

func (i *InodePool) InitInode() *Inode {
	ret := NewInode(i.ino)
	ret.addr.SetMem(unsafe.Pointer(ret))
	i.pool[ret.addr] = ret
	i.scatter.Push(ret)
	return ret
}

func (i *InodePool) next(lastest *Inode) *Inode {
	ret := &Inode{
		Ino:   lastest.Ino,
		Start: lastest.Start + Int32(len(lastest.Offsets)),
	}
	i.setPrevs(ret)

	ret.addr.SetMem(unsafe.Pointer(ret))
	i.pool[ret.addr] = ret

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
		i.pool[addr] = ino
	}
	return ino, nil
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
				addr := i.scatter[k+1].Prev
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
