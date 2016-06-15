package lfs

import "github.com/chzyer/logex"

type RootDir struct {
	volume *Volume
	fd     *File

	cache map[string]int32
}

func NewRootDir(v *Volume) (*RootDir, error) {
	fd, err := v.OpenFile("/", true)
	if err != nil {
		return nil, logex.Trace(err)
	}
	rd := &RootDir{
		volume: v,
		fd:     fd,
		cache:  make(map[string]int32),
	}
	return rd, nil
}

func (r *RootDir) Find(name string) int32 {
	if name == "/" {
		return 0
	}
	ret, ok := r.cache[name]
	if !ok {
		return -1
	}
	return ret
}

func (r *RootDir) Add(name string, ino int32) error {
	r.cache[name] = ino
	return nil
}
