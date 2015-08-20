package utils

import (
	"bytes"
	"os"
	"testing"

	"gopkg.in/logex.v1"
)

func TestReaderWriter(t *testing.T) {
	f, err := os.Create(GetRoot("/test/utils.reader"))
	if err != nil {
		t.Fatal(err)
	}

	w := NewWriter(f, 1)
	if offset, _ := w.Seek(-1, 1); offset != 0 {
		t.Fatal("writer seek work not as expected:", offset)
	}

	if _, err := w.Write([]byte("caoo")); err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	r := NewReader(f, 0)
	defer r.Close()

	if _, err := r.Seek(1, 1); err != nil {
		t.Fatal(err)
	}

	if _, err := r.Seek(0, 2); err == nil {
		t.Fatal("expect error")
	}

	buf := make([]byte, 3)
	if n, err := r.Read(buf); n != 3 || err != nil {
		t.Fatal("read not expected")
	}
	if !bytes.Equal(buf, []byte("aoo")) {
		t.Fatal("read result not expected")
	}
}

func TestBufio(t *testing.T) {
	var err error
	defer TDefer(t, &err)

	bio := NewBufioBlk([]byte("hello"))

	if bio.Offset(-1) != 0 {
		err = logex.NewError("offset not expected")
		return
	}
	bio.Offset(1)
	buf := make([]byte, 3)
	if n, err := bio.Read(buf); n != 3 || err != nil {
		err = logex.NewError("read fail")
		return
	}
	if !bytes.Equal(buf, []byte("ell")) {
		err = logex.NewError("read result not expected")
		return
	}

	if offset := bio.Offset(1); offset != 4 && bio.Offset(-1) != 1 {
		err = logex.NewError("seek fail", offset, bio.Offset(-1))
		return
	}

	bio.underlay.Close()
}

func TestBlock(t *testing.T) {
	var err error
	defer TDefer(t, &err)

	blk := NewBlock([]byte("hello"))
	n, err := blk.Read(make([]byte, 3))
	if err != nil {
		return
	}
	if n != 3 {
		err = logex.NewError("bytes read not expected")
		return
	}

	if !bytes.Equal(blk.Bytes(), []byte("lo")) {
		err = logex.NewError("bytes not expected")
		return
	}
}
