package bench

import (
	"crypto/rand"
	"fmt"
	"os"

	"github.com/chzyer/flow"
	"github.com/chzyer/madq/go/bio"
	"github.com/chzyer/madq/go/fs"
)

type FsFile struct {
	BenchCnt  int    `name:"count" desc:"bench size" default:"200"`
	BlockSize int    `name:"bs" desc:"block size" default:"200"`
	Dir       string `desc:"test directory path" default:"/tmp/madq"`
}

func (f *FsFile) FlaglyDesc() string {
	return "benchmark file Read/Write"
}

func (cfg *FsFile) FlaglyHandle(f *flow.Flow) error {
	defer f.Close()
	if err := os.RemoveAll(cfg.Dir); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	file, err := bio.NewFile(cfg.Dir)
	if err != nil {
		return fmt.Errorf("open volume: ", err)
	}
	defer file.Close()

	vol, err := fs.NewVolume(f, &fs.VolumeConfig{
		Delegate: bio.NewHybrid(file),
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

	for i := 0; i < cfg.BenchCnt; i++ {
		fd.Write(buf)
	}
	fd.Sync()

	println(fs.Stat.String())
	return nil
}
