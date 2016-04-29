package lfs

import (
	"testing"

	"github.com/chzyer/flow"
	"github.com/chzyer/madq/internal/bio"
	"github.com/chzyer/test"
)

func TestVolume(t *testing.T) {
	defer test.New(t)
	blk, err := bio.NewFile(test.Root())
	test.Nil(err)
	f := flow.New()

	vol, err := NewVolume(f, blk)
	test.Nil(err)
	defer vol.Close()

}
