package bitmap

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/chzyer/muxque/internal/utils"
	"gopkg.in/logex.v1"
)

func TestFileCon(t *testing.T) {
	n := 100
	dirname := "/data/mmq/test/bitmap.file.con"
	os.RemoveAll(dirname)

	f, err := NewFileEx(dirname, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	buf := []byte(utils.RandString(3))
	off := int64(0)
	for i := 0; i < n; i++ {
		n, err := f.WriteAt(buf, off)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(buf) {
			t.Fatal("short write", n, len(buf))
		}
		off += int64(n)
	}
}

func TestFile(t *testing.T) {
	dirname := "/data/mmq/test/bitmap.file"

	os.RemoveAll(dirname)
	f, err := NewFile(dirname)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf2 := make([]byte, 1024)
	copy(buf2, []byte("hello"))

	n, err := f.WriteAt(buf2[:5], 1024)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 1024)
	nRead, err := f.ReadAt(buf, 1024)
	if err != nil && !logex.Equal(err, io.EOF) {
		logex.Error(err)
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:nRead], buf2[:n]) {
		t.Fatal("result not expect")
	}

	{
		n, err := f.WriteAt(buf2[:5], int64(f.chunkSize)+11)
		if err != nil {
			t.Fatal(err)
		}
		nRead, err := f.ReadAt(buf, int64(f.chunkSize)+11)
		if err != nil && !logex.Equal(err, io.EOF) {
			t.Fatal(err)
		}
		if !bytes.Equal(buf[:nRead], buf2[:n]) {
			t.Fatal("result not expect")
		}
	}

	if f.Size() != int64(f.chunkSize)+11+5 {
		t.Fatal("size not expect")
	}
}
