package bitmap

import (
	"bytes"
	"io"
	"os"
	"testing"

	"gopkg.in/logex.v1"
)

func TestFile(t *testing.T) {
	dirname := os.TempDir()

	os.RemoveAll(dirname + "/bitmap_file/")
	f, err := NewFile(dirname + "/bitmap_file")
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
