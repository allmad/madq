package bench

import (
	"crypto/rand"
	"fmt"
	"os"
	"time"

	"github.com/allmad/madq/go/bio"
	"github.com/allmad/madq/go/fs"
	"github.com/allmad/madq/go/ptrace"
	"github.com/chzyer/flow"
	"github.com/chzyer/logex"
	"github.com/chzyer/test"
)

type FsFile struct {
	Trace     bool
	Mem       bool
	BenchCnt  int    `name:"count" desc:"bench size" default:"200"`
	BlockSize int    `name:"bs" desc:"block size" default:"200"`
	Stat      bool   `name:"stat"`
	RStat     bool   `name:"rstat"`
	Dir       string `desc:"test directory path" default:"/tmp/madq/bench/fsfile"`
}

func (f *FsFile) FlaglyDesc() string {
	return "benchmark file Read/Write"
}

func (cfg *FsFile) BenchWrite(f *flow.Flow, volcfg *fs.VolumeConfig, buf []byte) error {
	f = f.Fork(0)
	defer f.Close()

	if cfg.Trace {
		EnableTrace()
		defer DisableTrace()
	}

	now := time.Now()
	var size ptrace.Size
	size.AddInt(cfg.BlockSize * cfg.BenchCnt)

	defer func() {
		duration := time.Now().Sub(now)

		if cfg.Stat {
			println(fs.Stat.String())
		}
		println("write performance:", size.Rate(duration).String())
	}()

	vol, err := fs.NewVolume(f, volcfg)
	if err != nil {
		return err
	}
	defer vol.Close()

	fd, err := vol.Open("/hello", os.O_CREATE)
	if err != nil {
		return fmt.Errorf("error in openfile: %v", err)
	}
	defer fd.Close()

	for i := 0; i < cfg.BenchCnt; i++ {
		fd.Write(buf)
	}
	fd.Sync()
	return nil
}

func (cfg *FsFile) BenchRead(f *flow.Flow, volcfg *fs.VolumeConfig, expect []byte) error {
	f = f.Fork(0)
	defer f.Close()

	now := time.Now()
	var size ptrace.Size
	size.AddInt(cfg.BlockSize * cfg.BenchCnt)
	defer func() {
		duration := time.Now().Sub(now)

		if cfg.RStat {
			println(fs.Stat.String())
		}
		println("read performance:", size.Rate(duration).String())
	}()

	vol, err := fs.NewVolume(f, volcfg)
	if err != nil {
		return err
	}
	defer vol.Close()

	fd, err := vol.Open("/hello", 0)
	if err != nil {
		return fmt.Errorf("error in openfile: %v", err)
	}
	defer fd.Close()

	buf := make([]byte, len(expect))
	for i := 0; i < cfg.BenchCnt; i++ {
		_, err := fd.Read(buf)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cfg *FsFile) FlaglyHandle(f *flow.Flow) error {
	defer f.Close()

	buf := make([]byte, cfg.BlockSize)
	rand.Read(buf)

	volcfg := &fs.VolumeConfig{}

	if cfg.Mem {
		volcfg.Delegate = bio.NewHybrid(test.NewMemDisk(), fs.BlockBit)
	} else {
		vs, err := fs.NewVolumeSource(cfg.Dir)
		if err != nil {
			return err
		}
		defer vs.Close()
		volcfg.Delegate = vs
	}

	f.Add(1)
	defer f.Done()

	if err := cfg.BenchWrite(f, volcfg, buf); err != nil {
		return err
	}
	fs.ResetStat()
	if err := cfg.BenchRead(f, volcfg, buf); err != nil {
		logex.Error(err)
		return err
	}

	return nil
}
