package debug

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/allmad/madq/go/fs"
	"github.com/allmad/madq/go/ptrace"
	"github.com/chzyer/flagly"
)

type FSBrowserCmd struct {
	Stat  *FSBrowserCmdStat  `flagly:"handler"`
	Ls    *FSBrowserCmdList  `flagly:"handler"`
	Read  *FSBrowserCmdRead  `flagly:"handler"`
	Write *FSBrowserCmdWrite `flagly:"handler"`
	Cat   *FSBrowserCmdCat   `flagly:"handler"`
}

// -----------------------------------------------------------------------------

type FSBrowserCmdStat struct {
	Files []string `type:"[]"`
}

func (cfg *FSBrowserCmdStat) FlaglyHandle(vol *fs.Volume) {
	for _, f := range cfg.Files {
		fd, err := vol.Open(f, 0)
		if err != nil {
			println(err.Error())
			continue
		}
		cfg.StatFile(vol, fd)
		fd.Close()
	}
}

func (cfg *FSBrowserCmdStat) StatFile(vol *fs.Volume, fd *fs.Handle) {
	buf := bytes.NewBuffer(nil)
	inode, err := fd.Stat()
	if err != nil {
		println(err.Error())
		return
	}

	fmt.Fprintf(buf, "name: %v\n", fd.Name())
	fmt.Fprintf(buf, "mtime: %v\n", inode.Mtime.Get())
	fmt.Fprintf(buf, "size: %v (%v)\n", fd.Size(), ptrace.Unit(fd.Size()))

	os.Stderr.Write(buf.Bytes())
}

// -----------------------------------------------------------------------------

type FSBrowserCmdList struct{}

func (cfg *FSBrowserCmdList) FlaglyHandle(vol *fs.Volume) {
	for _, n := range vol.List() {
		println(n)
	}
}

// -----------------------------------------------------------------------------

type FSBrowserCmdRead struct {
	Fpath string `type:"[0]"`
	Off   int64  `type:"[1]"`
	N     int    `type:"[2]"`
}

func (cfg *FSBrowserCmdRead) FlaglyHandle(vol *fs.Volume) error {
	fd, err := vol.Open(cfg.Fpath, 0)
	if err != nil {
		return err
	}
	defer fd.Close()

	buf := make([]byte, cfg.N)
	if _, err := fd.ReadAt(buf, cfg.Off); err != nil {
		return err
	}
	fmt.Println(hex.Dump(buf))
	return nil
}

// -----------------------------------------------------------------------------

type FSBrowserCmdWrite struct {
	Fpath   string `type:"[0]"`
	Content string `type:"[1]"`
}

func (cfg *FSBrowserCmdWrite) FlaglyHandle(vol *fs.Volume) error {
	if cfg.Fpath == "" {
		return flagly.Errorf("fpath is required")
	}
	if cfg.Content == "" {
		return flagly.Errorf("content is empty")
	}
	fd, err := vol.Open(cfg.Fpath, os.O_CREATE)
	if err != nil {
		return err
	}
	defer fd.Close()

	if _, err := fd.Write([]byte(cfg.Content)); err != nil {
		return err
	}
	fd.Sync()

	return nil
}

// -----------------------------------------------------------------------------

type FSBrowserCmdCat struct {
	Fpath string `type:"[0]"`
}

func (cfg *FSBrowserCmdCat) FlaglyHandle(vol *fs.Volume) error {
	fd, err := vol.Open(cfg.Fpath, 0)
	if err != nil {
		return err
	}
	defer fd.Close()

	size := fd.Size()
	if size > 1024 {
		return fmt.Errorf("file size exceed 1024, can't use cat: %v", size)
	}
	return (&FSBrowserCmdRead{cfg.Fpath, 0, int(size)}).FlaglyHandle(vol)
}
