package lfs

import (
	"bytes"
	"encoding/hex"
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
		size := 200
		if len(b) > size {
			b = b[:size]
		}
		if len(buf) > size {
			buf = buf[:size]
		}
		print("readed:\n", hex.Dump(b))
		print("expect:\n", hex.Dump(buf))
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
		ins, err := newIns()
		if err != nil {
			return logex.Trace(err)
		}
		defer ins.Close()
		label := []string{
			"test1", "test2", "test3", "test4", "test5", "test6", "test7",
		}
		data := [][]byte{
			genBlock(2),
			genBlock(20),
			genBlock(9),
			genBlock(15),
			genBlock(ins.cfg.blkSize + 2),
			genBlock(2*ins.cfg.blkSize + 21),
			genBlock(ins.cfg.blkSize + 1),
		}

		testTime := 2

		for jj := 0; jj < testTime; jj++ { // write two file once
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

		for jj := 0; jj < testTime; jj++ { // read two file once
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

func TestNWriteMRead(t *testing.T) {
	if err := func() error {
		ins, err := newIns()
		if err != nil {
			return err
		}
		defer ins.Close()
		factor := 50

		w, err := ins.OpenWriter("test1")
		if err != nil {
			return err
		}
		defer w.Close()

		lengths := make([]int, factor)
		datas := make([][]byte, factor)
		expect := make([]byte, 0)
		total := 0
		for i := range lengths {
			l := 1 + utils.RandInt(200*cfg.blkSize)
			total += l
			datas[i] = bytes.Repeat([]byte(utils.RandString(1)), l)
			expect = append(expect, datas[i]...)
		}

		for _, data := range datas {
			if err := safeWrite(w, data); err != nil {
				return err
			}
		}

		r, err := ins.OpenReader("test1")
		if err != nil {
			return err
		}
		defer r.Close()

		maxSize := 1 << 12
		pStart := 0
		pEnd := maxSize
		for pEnd > pStart {
			if pEnd > len(expect) {
				pEnd = len(expect)
			}

			if err := safeReadExpect(r, expect[pStart:pEnd]); err != nil {
				return err
			}
			pStart = pEnd
			if pEnd < len(expect) {
				pEnd += maxSize
			}
		}

		// expect eof
		r.Offset -= 1
		n, err := r.Read([]byte("padding"))
		if !logex.Equal(err, io.EOF) || n != 1 {
			return logex.NewError("expect EOF, got ", n, err)
		}

		n, err = r.Read([]byte("pad"))
		if err != io.EOF || n != 0 {
			return logex.NewError("expect EOF, got ", n)
		}

		return nil
	}(); err != nil {
		logex.Error(err)
		t.Fatal(err)
	}
}
