package fs

import (
	"os"
	"testing"
	"time"

	"github.com/allmad/madq/go/bio"
	"github.com/chzyer/flow"
	"github.com/chzyer/test"
)

func TestVolume(t *testing.T) {
	defer test.New(t)

	delegate := bio.NewHybrid(test.NewMemDisk(), BlockBit)
	vol, err := NewVolume(flow.New(), &VolumeConfig{
		Delegate:      delegate,
		FlushInterval: time.Second,
		FlushSize:     16 << 20,
	})
	defer vol.Close()
	test.Nil(err)

	{ // open without create
		fd, err := vol.Open("hello", 0)
		test.Nil(fd)
		test.Equal(ErrFileNotExist, err)
	}

	{
		fd, err := vol.Open("hello", os.O_CREATE)
		test.Nil(err)
		test.Equal(fd.Ino(), int32(1))
		fd.Write([]byte("hello"))
		fd.Sync()
		fd.Close()
		test.Nil(vol.FlushInodeMap())

		vol.CleanCache()
		fd, err = vol.Open("hello", 0)
		test.Nil(err)
		test.ReadStringAt(fd, 0, "hello")
	}

	_ = vol
}
