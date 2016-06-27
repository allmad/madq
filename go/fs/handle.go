package fs

type Handle struct {
	*File
	offset int64
}

func NewHandle(f *File, off int64) *Handle {
	return &Handle{
		File:   f,
		offset: off,
	}
}

func (f *Handle) Read(b []byte) (int, error) {
	n, err := f.File.ReadAt(b, f.offset)
	f.offset += int64(n)
	return n, err
}
