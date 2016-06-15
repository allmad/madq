package bio

import (
	"testing"

	"github.com/chzyer/test"
)

func TestDevice(t *testing.T) {
	defer test.New(t)
	test.CleanTmp()

	f, err := test.TmpFile()
	test.Nil(err)
	defer f.Close()
	prefix := make([]byte, 256)
	for i := range prefix {
		prefix[i] = byte(i)
	}
	_, err = f.WriteAt(prefix, 0)
	test.Nil(err)

	dev := NewDevice(f, int64(len(prefix)))
	ret := make([]byte, 2)
	{ // hybird, memory got nothing
		n, err := dev.ReadAt(ret, 255)
		test.Equal(n, 1)
		test.Nil(err)
		test.Equal(ret, []byte{255, 0})
	}

	{ // write to disk and memory
		ret = []byte{1, 2}
		_, err := dev.WriteAt(ret, 255)
		test.Equal(err, ErrDeviceWriteback)
	}

	{ // write to memory [1, 2]
		n, err := dev.WriteAt(ret, 256)
		test.Equal(n, len(ret))
		test.Nil(err)
	}

	{ // read hybird
		buf := []byte{0, 0, 0, 0}
		n, err := dev.ReadAt(buf, 255)
		test.Equal(n, 3)
		test.Nil(err)
		test.Equal(buf, []byte{255, 1, 2, 0})
	}

	{ // buffered()
		buffered := dev.Buffered()
		test.Equal(buffered, 2)
	}

	{ // flush, [0, ..., 255, 1 ,2]
		err := dev.Flush()
		test.Nil(err)
		fi, err := f.Stat()
		test.Nil(err)
		test.Equal(fi.Size(), 256+2)
		expect := append(prefix, 1, 2)
		got := make([]byte, len(expect))
		n, err := f.ReadAt(got, 0)
		test.Equal(n, len(expect))
		test.Nil(err)
		test.Equal(expect, got)
	}

	{ // buffered()
		buffered := dev.Buffered()
		test.Equal(buffered, 0)
	}

}
