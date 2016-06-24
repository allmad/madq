package fs

import (
	"testing"

	"github.com/chzyer/flow"
	"github.com/chzyer/madq/go/bio"
	"github.com/chzyer/test"
)

func TestVolume(t *testing.T) {
	defer test.New(t)

	delegate := bio.NewHybrid(test.NewMemDisk())
	vol, err := NewVolume(flow.New(), &VolumeConfig{
		Delegate: delegate,
	})
	defer vol.Close()
	test.Nil(err)

	{ // open without create
		fd, err := vol.Open("hello", 0)
		test.Nil(fd)
		test.Equal(ErrFileNotExist, err)
	}
	_ = vol
}
