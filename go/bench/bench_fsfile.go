package bench

import (
	"crypto/rand"
	"fmt"
	"os"
	"time"

	"github.com/chzyer/flow"
	"github.com/chzyer/madq/go/fs"
	"github.com/chzyer/madq/go/ptrace"
)

type FsFile struct {
	BenchCnt  int    `name:"count" desc:"bench size" default:"200"`
	BlockSize int    `name:"bs" desc:"block size" default:"200"`
	Dir       string `desc:"test directory path" default:"/tmp/madq/bench/fsfile"`
}

func (f *FsFile) FlaglyDesc() string {
	return "benchmark file Read/Write"
}

func (cfg *FsFile) FlaglyHandle(f *flow.Flow) error {
	defer f.Close()

	now := time.Now()

	vs, err := fs.NewVolumeSource(cfg.Dir)
	if err != nil {
		return err
	}
	defer vs.Close()

	vol, err := fs.NewVolume(f, &fs.VolumeConfig{
		Delegate: vs,
	})
	if err != nil {
		return err
	}
	defer vol.Close()

	fd, err := vol.Open("/hello", os.O_CREATE)
	if err != nil {
		return fmt.Errorf("error in openfile: %v", err)
	}
	defer fd.Close()

	buf := make([]byte, cfg.BlockSize)
	rand.Read(buf)
	var size ptrace.Size

	size.AddInt(len(buf) * cfg.BenchCnt)

	for i := 0; i < cfg.BenchCnt; i++ {
		fd.Write(buf)
	}
	fd.Sync()
	duration := time.Now().Sub(now)

	println(fs.Stat.String())
	println(size.Rate(duration).String())
	return nil
}
