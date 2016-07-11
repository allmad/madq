package bench

import (
	"crypto/rand"
	"time"

	"github.com/allmad/madq/go/bio"
	"github.com/allmad/madq/go/ptrace"
	"github.com/chzyer/flow"
)

type RawDisk struct {
	Fpath string `default:"/tmp/madq/bench/rawdisk"`
	BS    int    `default:"200"`
	Count int    `default:"200"`
}

func (r *RawDisk) FlaglyHandle(f *flow.Flow) error {
	defer f.Close()

	fd, err := bio.NewFile(r.Fpath)
	if err != nil {
		return err
	}
	defer fd.Close()

	now := time.Now()
	var size ptrace.Size
	buf := make([]byte, r.BS)
	size.AddInt(r.BS * r.Count)
	rand.Read(buf)
	n := int64(0)
	for i := 0; i < r.Count; i++ {
		fd.WriteAt(buf, n)
		n += int64(len(buf))
	}
	d := time.Now().Sub(now)
	println(size.Rate(d).String())
	return nil
}
