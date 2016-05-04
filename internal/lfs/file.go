package lfs

type File struct {
	name   string
	ino    int32
	v      *Volume
	offset int64
}

func NewFile(v *Volume, ino int32, name string) (*File, error) {
	fd := &File{
		v:    v,
		ino:  ino,
		name: name,
	}
	return fd, nil
}

func (f *File) Read(b []byte) (n int, err error) {
	n, err = f.ReadAt(b, f.offset)
	f.offset += int64(n)
	return n, err
}

func (f *File) ReadAt(b []byte, offset int64) (n int, err error) {
	return
}

func (f *File) Write(b []byte) (n int, err error) {
	return
}

func (f *File) Name() string {
	return f.name
}

func (f *File) Close() error {
	return nil
}
