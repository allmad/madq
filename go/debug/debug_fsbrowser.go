package debug

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/flow"
	"github.com/chzyer/madq/go/bio"
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
		switch sp[0] {
		case "ls":
			cfg.List(vol)
		case "stat":
			cfg.Stat(vol, sp[1:])
		default:
			println("unknown commmand:", line.Line)
		}
	}
	return nil
}

func (cfg *FSBrowser) Stat(vol *fs.Volume, files []string) {
	for _, f := range files {
		fd, err := vol.Open(f, 0)
		if err != nil {
			println(err.Error())
			continue
		}
		cfg.StatFile(vol, fd)
	}
}

func (cfg *FSBrowser) StatFile(vol *fs.Volume, fd *fs.File) {
	fmt.Fprintf(os.Stderr,
		"name: %v\nsize: %v\n",
		fd.Name(), fd.Size(),
	)
}

func (cfg *FSBrowser) List(vol *fs.Volume) {
	for _, n := range vol.List() {
		println(n)
	}
}
