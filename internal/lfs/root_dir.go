package lfs

import "github.com/chzyer/logex"

type RootDir struct {
	volume *Volume
	fd     *File
}

func NewRootDir(v *Volume) (*RootDir, error) {
	fd, err := v.OpenFile("/", false)
	if err != nil {
		return nil, logex.Trace(err)
	}
	rd := &RootDir{
		volume: v,
		fd:     fd,
	}
	return rd, nil
}
