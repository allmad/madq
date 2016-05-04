package lfs

import "github.com/chzyer/logex"

type RootDir struct {
	volume *Volume
	fd     *File
}

func NewRootDir(v *Volume) (*RootDir, error) {
	fd, err := v.OpenFile("/", true)
	if err != nil {
		return nil, logex.Trace(err)
	}
	rd := &RootDir{
		volume: v,
		fd:     fd,
	}
	return rd, nil
}

func (r *RootDir) Find(name string) int32 {
	return -1
}

func (r *RootDir) Add(name string, ino int32) error {
	println("hello")
	return nil
}
