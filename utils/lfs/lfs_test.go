package lfs

import (
	"bytes"
	"io"
	"testing"

	"gopkg.in/logex.v1"

	"github.com/chzyer/muxque/utils"
)

func safeReadExpect(r io.Reader, buf []byte) error {
	b := make([]byte, len(buf))
	if err := safeRead(r, b); err != nil {
		return logex.Trace(err)
	}
	if !bytes.Equal(b, buf) {
		logex.Info(b)
		logex.Info(buf)
		return logex.NewError("read data not expect")
	}
	return nil
}

func safeRead(r io.Reader, buf []byte) error {
	n, err := r.Read(buf)
	if err != nil {
		return logex.Trace(err)
	}
	if n != len(buf) {
		return logex.NewError("read length not expect")
	}
	return nil
}

func safeWrite(w io.Writer, buf []byte) error {
	n, err := w.Write(buf)
	if err != nil {
		return logex.Trace(err)
	}
	if n != len(buf) {
		return logex.NewError("write length not match")
	}
	return nil
}

var cfg = &Config{
	BasePath: utils.GetRoot("/test/lfs"),
}

func genBlock(size int) []byte {
	return bytes.Repeat([]byte(utils.RandString(1)), size)
}

func newIns() (*Ins, error) {
	ins, err := New(cfg)
	if err != nil {
		return nil, logex.Trace(err)
	}
	ins.Pruge()
	return New(cfg)
}

func TestSingleRW(t *testing.T) {
	testData := genBlock(9 << 10)
	ins, err := newIns()
	if err != nil {
		t.Fatal(err)
	}
	defer ins.Pruge()

	f, err := ins.Open("lfsTestTopic")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	w := &utils.Writer{f, 0}
	if err := safeWrite(w, testData); err != nil {
		logex.Error(err)
		t.Fatal()
	}

	testDataBuf := make([]byte, len(testData))
	r := utils.NewBufio(&utils.Reader{f, 0})
	if err := safeRead(r, testDataBuf); err != nil {
		logex.Error(err)
		t.Fatal()
	}
}

func TestTwoRW(t *testing.T) {
	if err := func() error {
		data1 := genBlock(20)
		data2 := genBlock(38)
		ins, err := newIns()
		if err != nil {
			return logex.Trace(err)
		}
		defer ins.Close()

		{ // write two file once
			w1, err := ins.OpenWriter("test1")
			if err != nil {
				return logex.Trace(err)
			}
			defer w1.Close()

			w2, err := ins.OpenWriter("test2")
			if err != nil {
				return logex.Trace(err)
			}
			defer w2.Close()

			if err := safeWrite(w1, data1); err != nil {
				return logex.Trace(err)
			}

			if err := safeWrite(w1, data2); err != nil {
				return logex.Trace(err)
			}
		}

		{ // read two file once
			r1, err := ins.OpenReader("test1")
			if err != nil {
				return logex.Trace(err)
			}
			defer r1.Close()
			r2, err := ins.OpenReader("test2")
			if err != nil {
				return logex.Trace(err)
			}
			defer r2.Close()

			if err := safeReadExpect(r2, data2); err != nil {
				return logex.Trace(err)
			}
			if err := safeReadExpect(r1, data1); err != nil {
				return logex.Trace(err)
			}
		}
		return nil
	}(); err != nil {
		logex.DownLevel(1).Error(err)
		t.Fatal(err)
	}
}
