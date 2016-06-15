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
	ino      int32
	scatter  InodeScatter
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

func (i *InodePool) RefPayloadBlock() (*Inode, int, error) {
	inode, err := i.GetLastest()
	if err != nil {
		return nil, -1, logex.Trace(err)
	}
	idx := inode.GetOffsetIdx()
	if idx < len(inode.Offsets)-1 {
		return inode, idx, nil
	}

	// old inode is full
	return i.next(inode), idx, nil
}

func (i *InodePool) next(ino *Inode) *Inode {
	ret := &Inode{
		Ino:   ino.Ino,
		Start: ino.Start + Int32(len(ino.Offsets)),
	}
	ret.addr.SetMem(unsafe.Pointer(ret))
	return ret
}

// get newest inode from memory or disk
func (i *InodePool) GetLastest() (*Inode, error) {
	inode := i.scatter.Current()
	if inode != nil {
		return inode, nil
	}
	inode, err := i.delegate.GetInode(i.ino)
	if err != nil {
		return nil, logex.Trace(err)
	}
	i.scatter.Push(inode)
	return inode, nil
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

type InodeScatter [32]*Inode

func (is *InodeScatter) Push(i *Inode) {
	copy(is[:], is[1:])
	is[len(is)-1] = i
}

func (s *InodeScatter) Current() *Inode {
	return s[len(s)-1]
}
