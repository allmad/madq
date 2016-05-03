package bio

type RawDisker interface {
	ReadAt(b []byte, off int64) (n int, err error)
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
	ReadDisk(Diskable) error
	Skip(n int)
}
type DiskWriter interface {
	Byte([]byte)
	Int32(int32)
	Int64(int64)
	Skip(n int)
}
