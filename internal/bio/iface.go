package bio

import "encoding/hex"

type RawDiskerReader interface {
	ReadAt(b []byte, off int64) (n int, err error)
}

type RawDisker interface {
	RawDiskerReader
	WriteAt(b []byte, off int64) (n int, err error)
}

type Diskable interface {
	Size() int
	ReadDisk(r DiskReader) error
	WriteDisk(w DiskWriter)
}

type DiskReader interface {
	Verify(b []byte) bool
	Int32() int32
	Int64() int64
	Byte(n int) []byte
	ReadDisk(Diskable) error
	Skip(n int)
	Offset() int
}
type DiskWriter interface {
	Byte([]byte) int
	Int32(int32)
	Int64(int64)
	Skip(n int)
}

func Dump(d Diskable) string {
	ret := make([]byte, d.Size())
	d.WriteDisk(NewWriter(ret))
	return hex.Dump(ret)
}

func Bin(i int64) string {
	b := make([]byte, 8)
	NewWriter(b).Int64(i)
	return hex.Dump(b)
}
