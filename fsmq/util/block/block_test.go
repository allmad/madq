package block

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/chzyer/fsmq/fsmq/util/test"
)

var (
	b *Instance
	_ io.ReaderAt = b
	_ io.WriterAt = b

	root = test.GetRoot("/test/util/block")
)

func TestBlockException(t *testing.T) {
	defer test.New(t)

	_, err := New(root, 1<<33)
	test.NotNil(err)

	_, err = New("/usr/bin/notperm", 11)
	test.NotNil(err)

	f, err := New(root, 10)
	test.Nil(err)

	err = f.Delete(false)
	test.Nil(err)

	_, err = f.ReadAt(nil, -1)
	test.Equal(err, ErrInvalidOffset)
	test.Equal(f.Close(), true)
	test.Equal(f.Close(), false)
	test.Equal(f.Delete(false), nil)
	_, err = f.WriteAt(nil, 1)
	test.Equal(err, ErrBlockClosed)
	_, err = f.ReadAt(nil, 1)
	test.Equal(err, ErrBlockClosed)
}

func TestBlockWriteRead(t *testing.T) {
	defer test.New(t)

	f, err := New(root, 4)
	test.Nil(err)
	defer f.Close()

	buf := []byte("abcdefgh")

	for i := 0; i < 1024; i += 8 {
		n, err := f.WriteAt(buf, int64(i))
		test.Equals(n, 8, err, nil)
	}

	buf2 := make([]byte, len(buf))
	for i := 0; i < 1024; {
		n, err := f.ReadAt(buf2, int64(i))
		test.Equals(n, len(buf2), buf2, buf, err, nil)
		i += n
	}
}

func TestBlock(t *testing.T) {
	defer test.New(t)

	f, err := New(root, 2)
	test.Nil(err)
	test.Nil(f.Delete(false))
	defer f.Close()

	buf := bytes.Repeat([]byte("ha"), 1024)
	n, err := f.WriteAt(buf, 1)
	test.Nil(err)
	test.Equal(n, len(buf))

	buf2 := make([]byte, len(buf))
	n, err = f.ReadAt(buf2, 0)
	test.Nil(err)
	test.Equal(n, len(buf2))
	test.Equal(buf[:len(buf)-1], buf2[1:])

	n, err = f.ReadAt([]byte(" "), 1024*2+1)
	test.Equals(n, 0, err, io.EOF)

	n, err = f.ReadAt([]byte("  "), 0)
	test.Equals(n, 2, err, nil)

	n, err = f.ReadAt([]byte("  "), 0)
	test.Equals(n, 2, err, nil)

	os.RemoveAll(root)
	n, err = f.ReadAt([]byte("  "), 4)
	test.Equals(n, 0)
	test.CheckError(err, test.StrNotSuchFile)

	n, err = f.WriteAt([]byte("  "), 4)
	test.Equals(n, 0)
	test.CheckError(err, test.StrNotSuchFile)
}
