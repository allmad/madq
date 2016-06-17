package fs

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

var _ DiskItem = new(Int32)

type Int32 int32

func (Int32) DiskSize() int {
	return 4
}

func (v *Int32) ReadDisk(w []byte) error {
	n := binary.BigEndian.Uint32(w)
	*v = Int32(n)
	return nil
}

func (v Int32) WriteDisk(w []byte) {
	binary.BigEndian.PutUint32(w, uint32(v))
}

// -----------------------------------------------------------------------------

var _ DiskItem = new(Address)

type Address int64

func (a *Address) Set(to Address) {
	*a = to
}

func (a *Address) String() string {
	return fmt.Sprint(*a)
}

func (Address) DiskSize() int { return 8 }

func (a Address) WriteDisk(w []byte) {
	binary.BigEndian.PutUint64(w, uint64(a))
}

func (a *Address) ReadDisk(b []byte) error {
	n := binary.BigEndian.Uint64(b)
	*a = Address(n)
	return nil
}

func (a *Address) SetMem(p unsafe.Pointer) {
	*a = -Address(uintptr(p))
}

func (a Address) IsEmpty() bool {
	return a == 0
}

func (a Address) IsInMem() bool {
	return a < 0
}

// -----------------------------------------------------------------------------

var _ DiskItem = new(ShortAddr)

type ShortAddr int64

func (ShortAddr) DiskSize() int { return 6 }

func (a ShortAddr) WriteDisk(b []byte) {
	binary.BigEndian.PutUint16(b[:2], uint16(a>>32))
	binary.BigEndian.PutUint32(b[2:], uint32(a))
}

func (a *ShortAddr) ReadDisk(b []byte) error {
	n := binary.BigEndian.Uint16(b[:2])
	n2 := binary.BigEndian.Uint32(b[2:])
	*a = ShortAddr((int64(n) << 32) + int64(n2))
	return nil
}
