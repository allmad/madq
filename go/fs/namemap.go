package fs

import (
	"fmt"
	"strings"
)

const FileNameSize = 28

type FileName [FileNameSize]byte

func (f *FileName) String() string {
	return strings.TrimRight(string(f[:]), "\x00")
}

// NameMap is a File which ino is 0
// fd close by NameMap
type NameMap struct {
	fh      *Handle
	cache   map[FileName]int32
	useIno  map[int32]struct{}
	freeIno int32
}

func NewNameMap(fh *Handle, start int32) (*NameMap, error) {
	fsize := fh.Size()
	n := fsize / NameMapItemSize
	if n < 1024 {
		n = 1024
	}
	nm := &NameMap{
		fh:      fh,
		cache:   make(map[FileName]int32, n),
		useIno:  make(map[int32]struct{}, n),
		freeIno: start,
	}
	if err := nm.init(); err != nil {
		return nil, err
	}
	return nm, nil
}

func (n *NameMap) List() []string {
	list := make([]string, 0, len(n.cache))
	for k, ino := range n.cache {
		list = append(list, fmt.Sprintf("%v\t%v", k.String(), ino))
	}
	return list
}

func (n *NameMap) checkIno(ino int32) {
	if ino == n.freeIno {
		n.freeIno++
		for {
			if _, ok := n.useIno[n.freeIno]; !ok {
				break
			}
		}
	}
}

func (n *NameMap) GetFreeIno() (int32, error) {
	ino := n.freeIno
	if ino < 0 {
		return -1, fmt.Errorf("not free ino")
	}
	n.checkIno(n.freeIno)
	return ino, nil
}

func (n *NameMap) init() error {
	fsize := n.fh.Size()
	size := int(fsize / NameMapItemSize)

	var buf [32]byte
	var item NameMapItem
	for i := 0; i < size; i++ {
		_, err := n.fh.Read(buf[:])
		if err != nil {
			return err
		}
		if err := (&item).ReadDisk(buf[:]); err != nil {
			return err
		}
		n.checkIno(int32(item.Ino))
		n.cache[item.Name] = int32(item.Ino)
	}
	return nil
}

func (n *NameMap) AddIno(name string, ino int32) error {
	fn, err := n.getName(name)
	if err != nil {
		return err
	}

	if _, ok := n.cache[fn]; ok {
		return fmt.Errorf("file is already exists")
	}

	n.cache[fn] = ino
	n.useIno[ino] = struct{}{}
	buf := make([]byte, NameMapItemSize)
	(&NameMapItem{fn, Int32(ino)}).WriteDisk(buf)

	if _, err := n.fh.Write(buf); err != nil {
		return err
	}
	n.fh.Sync()
	return nil
}

// ino: -1 means file not found
func (n *NameMap) GetIno(name string) (ino int32, err error) {
	fn, err := n.getName(name)
	if err != nil {
		return -1, err
	}

	ino, ok := n.cache[fn]
	if !ok {
		ino = -1
	}
	return ino, nil
}

func (n *NameMap) getName(name string) (FileName, error) {
	var fn FileName
	if len(fn) < len(name) {
		return fn, fmt.Errorf("filename too long")
	}
	copy(fn[:], []byte(name))
	return fn, nil
}

func (n *NameMap) Close() {
	n.fh.Close()
}

// -----------------------------------------------------------------------------

const (
	NameMapItemSize = 32
)

type NameMapItem struct {
	Name FileName
	Ino  Int32
}

func (n *NameMapItem) DiskSize() int {
	return NameMapItemSize
}

func (n *NameMapItem) ReadDisk(b []byte) error {
	copy(n.Name[:], b[:FileNameSize])
	n.Ino.ReadDisk(b[FileNameSize:])
	return nil
}

func (n *NameMapItem) WriteDisk(b []byte) {
	copy(b[:FileNameSize], n.Name[:])
	n.Ino.WriteDisk(b[FileNameSize:])
}
