package debug

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/flagly"
	"github.com/chzyer/flow"
	"github.com/chzyer/madq/go/bio"
	"github.com/chzyer/madq/go/common"
	"github.com/chzyer/madq/go/fs"
	"github.com/chzyer/readline"
)

type FSBrowser struct {
	Dir string `type:"[0]" desc:"directory"`
}

func (cfg *FSBrowser) FlaglyHandle(f *flow.Flow) error {
	defer f.Close()

	if cfg.Dir == "" {
		return fmt.Errorf("error: directory is required")
	}

	if _, err := os.Stat(cfg.Dir); os.IsNotExist(err) {
		return err
	}

	flock, err := common.NewFlock(cfg.Dir)
	if err != nil {
		return err
	}
	defer flock.Unlock()

	fd, err := bio.NewFile(cfg.Dir)
	if err != nil {
		return err
	}
	vol, err := fs.NewVolume(f, &fs.VolumeConfig{
		Delegate: bio.NewHybrid(fd),
	})
	if err != nil {
		return err
	}
	defer vol.Close()

	return cfg.handle(filepath.Base(cfg.Dir), vol)
}

func (cfg *FSBrowser) handle(name string, vol *fs.Volume) error {
	rl, err := readline.New(name + "> ")
	if err != nil {
		return err
	}
	defer rl.Close()

	for {
		line := rl.Line()
		if line.CanBreak() {
			break
		} else if line.CanContinue() {
			continue
		}
		sp := strings.Fields(line.Line)
		fs := flagly.New("")
		fs.Context(vol)
		if err := fs.Compile(&FSBrowserCmd{}); err != nil {
			return err
		}
		if err := fs.Run(sp); err != nil {
			println(err.Error())
			continue
		}
	}
	return nil
}
