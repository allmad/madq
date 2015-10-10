package block

import (
	"bytes"
	"io"
	"testing"

	"gopkg.in/logex.v1"

	"github.com/chzyer/fsmq/fsmq/util"
)

var (
	b *Instance
	_ io.ReaderAt = b
	_ io.WriterAt = b

	root = util.GetRoot("/test/util/block")
)

func TestBlock(t *testing.T) {
	var err error
	defer util.Defer(t, &err)

	f, err := New(root, 2)
	if err != nil {
		return
	}
	defer f.Close()

	buf := bytes.Repeat([]byte("hh"), 1024)
	n, err := f.WriteAt(buf, 1)
	if err != nil {
		return
	}
	if n != len(buf) {
		err = logex.NewError("result not expect")
		return
	}
}
