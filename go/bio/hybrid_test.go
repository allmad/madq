package bio

import (
	"testing"

	"github.com/chzyer/test"
)

func TestHybrid(t *testing.T) {
	defer test.New(t)

	fd, err := NewFile(test.Root())
	test.Nil(err)
	defer fd.Close()

	buf := test.SeqBytes(128)
	_, err = fd.WriteAt(buf, 0)
	test.Nil(err)

	h := NewHybrid(fd, 4)
	remain := len(buf)
	min := func(a, b int) int {
		if a > b {
			return b
		}
		return a
	}
	off := int64(0)
	const n = 10
	for remain > 0 {
		ret, err := h.ReadData(off, min(remain, n))
		test.Nil(err)
		test.EqualBytes(ret, buf[off:off+int64(min(remain, n))])
		remain -= n
		off += n
	}
}
