package fs

import (
	"testing"

	"github.com/chzyer/flow"
	"github.com/chzyer/test"
)

func TestVolume(t *testing.T) {
	defer test.New(t)

	delegate := test.NewMemDisk()
	vol, err := NewVolume(flow.New(), &VolumeConfig{
		Delegate: delegate,
	})
	test.Nil(err)
	_ = vol
}
