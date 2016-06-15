package lfs

import (
	"testing"

	"github.com/chzyer/flow"
	"github.com/chzyer/madq/internal/bio"
	"github.com/chzyer/test"
)

func TestVolume(t *testing.T) {
	defer test.New(t)
	test.CleanTmp()

	blk, err := bio.NewFile(test.Root())
	test.Nil(err)
	blk.Delete(false)
	f := flow.New()

	vol, err := NewVolume(f, blk)
	test.Nil(err)
	defer vol.Close()

	{
		_, err := vol.OpenFile("hello", false)
		test.Equal(err, ErrVolumeFileNotExists)
	}

	{
		f, err := vol.OpenFile("hello", true)
		test.Nil(err)
		test.NotNil(f)
		test.WriteString(f, "hello")
		test.WriteString(f, " bye")
		test.ReadString(f, "hello")
		f.Flush()
	}
}
