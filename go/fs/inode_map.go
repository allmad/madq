package fs

import (
	"fmt"
	"io"
	"sync"

	"github.com/chzyer/logex"
	"github.com/chzyer/madq/go/bio"
)

const InodeMapCap = 1 << 20
const InodeMapSize = 6 * InodeMapCap

type InodeMap struct {
	offset   ShortAddr
	delegate InodeMapDelegate
	InoMap   []byte
	m        sync.Mutex
}

type InodeMapDelegate interface {
	bio.ReadWriterAt
}

func NewInodeMap(offset int64, delegate InodeMapDelegate, create bool) (*InodeMap, error) {
	m := &InodeMap{
		offset:   ShortAddr(offset),
		delegate: delegate,
		InoMap:   make([]byte, InodeMapSize),
	}

	n, err := delegate.ReadAt(m.InoMap, offset)
	if err != nil {
		if !logex.Equal(err, io.EOF) || !create {
			return nil, logex.Trace(err)
		}
		return m, nil
	}
	if n != len(m.InoMap) {
		return nil, fmt.Errorf("read inodemap: short read")
	}
	return m, nil
}

func (m *InodeMap) getData(ino int32) ([]byte, error) {
	if int(ino) >= InodeMapCap {
		return nil, fmt.Errorf("invalid inode number: %v", ino)
	}

	return m.InoMap[ino*6 : (ino+1)*6], nil
}

func (m *InodeMap) GetInodeByAddr(addr Address) (*Inode, error) {
	inode := NewInode(-1)
	if err := ReadDisk(m.delegate, inode, addr); err != nil {
		return nil, err
	}
	return inode, nil
}

func (m *InodeMap) GetInode(ino int32) (*Inode, error) {
	m.m.Lock()
	defer m.m.Unlock()

	addrData, err := m.getData(ino)
	if err != nil {
		return nil, err
	}
	var addr ShortAddr
	_ = addr.ReadDisk(addrData)
	if addr.IsEmpty() {
		return nil, fmt.Errorf("fild not found: (ino: %v)", ino)
	}

	inode := NewInode(ino)
	if err := ReadDisk(m.delegate, inode, Address(addr)); err != nil {
		return nil, err
	}
	return inode, nil
}

func (m *InodeMap) SaveInode(inode *Inode) {
	if inode.addr.IsInMem() {
		panic("can't save inode which is not flushed yet")
	}

	m.m.Lock()
	addrData, err := m.getData(int32(inode.Ino))
	if err != nil {
		panic("inode number exceed cap")
	}
	ShortAddr(inode.addr).WriteDisk(addrData)
	m.m.Unlock()
}

func (m *InodeMap) DiskSize() int {
	return 6 * (1 << 30)
}

// TODO(chzyer): add replica
func (m *InodeMap) Flush() error {
	m.m.Lock()
	_, err := m.delegate.WriteAt(m.InoMap, int64(m.offset))
	m.m.Unlock()
	return err
}
