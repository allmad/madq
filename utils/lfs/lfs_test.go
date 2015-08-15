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
		logex.Info("readed:", b)
		logex.Info("expect:", buf)
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
	t.Skip()
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
		ins, err := newIns()
		if err != nil {
			return logex.Trace(err)
		}
		defer ins.Close()
		label := []string{"test1", "test2", "test3", "test4", "test5"}
		data := [][]byte{genBlock(2), genBlock(20), genBlock(9), genBlock(15), genBlock(ins.cfg.blockSize + 2)}

		{ // write two file once
			ws := make([]*utils.Writer, len(label))
			for i, l := range label {
				w, err := ins.OpenWriter(l)
				if err != nil {
					return logex.Trace(err)
				}
				ws[i] = w
				defer w.Close()
			}

			for i := range label {
				if err := safeWrite(ws[i], data[i]); err != nil {
					return logex.Trace(err)
				}
			}
		}

		{ // read two file once
			rs := make([]*utils.Reader, len(label))
			for i, l := range label {
				r, err := ins.OpenReader(l)
				if err != nil {
					return logex.Trace(err)
				}
				defer r.Close()
				rs[i] = r
			}

			for i := range label {
				if err := safeReadExpect(rs[i], data[i]); err != nil {
					return logex.Trace(err)
				}
			}
		}
		return nil
	}(); err != nil {
		logex.DownLevel(1).Error(err)
		t.Fatal(err)
	}
}
