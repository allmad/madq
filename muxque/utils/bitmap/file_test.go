package bitmap

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/chzyer/fsmq/utils"

	"gopkg.in/logex.v1"
)

func BenchmarkWrite256(b *testing.B) {
	benchmarkSize(b, 256)
}
func BenchmarkWrite256N(b *testing.B) {
	b.Skip()
	benchmarkFileSize(b, 256)
}

func BenchmarkWriteBig256(b *testing.B) {
	benchmarkSize(b, 256*100)
}

func benchmarkFileSize(b *testing.B, size int) {
	fileName := utils.GetRoot("/test/bitmap/normalfile")
	os.RemoveAll(fileName)

	f, _ := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0600)
	b.SetBytes(int64(size))
	data := make([]byte, size)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		f.Write(data)
	}
	f.Sync()
}

func benchmarkSize(b *testing.B, size int) {
	fileName := utils.GetRoot("/test/bitmap/benchmark")
	os.RemoveAll(fileName)

	f, err := NewFileEx(fileName, 22)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		f.Delete()
	}()

	b.ResetTimer()
	data := []byte(strings.Repeat(utils.RandString(1), size))
	w := utils.Writer{f, 0}
	for i := 0; i < b.N; i++ {
		w.Write(data)
		b.SetBytes(int64(len(data)))
	}
}

func TestFileCon(t *testing.T) {
	n := 100
	dirname := utils.GetRoot("/test/bitmap.file.con")
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

func TestFileName(t *testing.T) {
	if GetName(10, 1024) != "1" {
		t.Fatal("result not expect")
	}
	if GetName(10, 10) != "0" {
		t.Fatal("result not expect")
	}
}

func TestFileOp(t *testing.T) {
	dirname := utils.GetRoot("/test/bitmap.file")

	f, err := NewFile(dirname)
	if err != nil {
		t.Fatal(err)
	}
	f.Delete()
	if _, err := os.Stat(dirname); os.IsExist(err) {
		t.Fatal("expect not exist")
	}

	f, err = NewFileEx(dirname, 11)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf2 := make([]byte, 1024)
	copy(buf2, []byte("hello"))
	buf := make([]byte, 1024)

	{
		n, err := f.WriteAt(buf2[:5], int64(f.chunkSize)-3)
		if err != nil {
			t.Fatal(err)
		}
		nRead, err := f.ReadAt(buf[:5], int64(f.chunkSize)-3)
		if err != nil && !logex.Equal(err, io.EOF) {
			t.Fatal(err)
		}
		if !bytes.Equal(buf[:nRead], buf2[:n]) {
			t.Fatal("result not expect")
		}
	}

	if f.Size() != int64(f.chunkSize)-3+5 {
		t.Fatal("size not expect")
	}

	for i := 0; i < 20; i++ {
		offset := int64(utils.RandInt(4096))
		n, err := f.WriteAt(buf2[:5], 1024+offset)
		if err != nil {
			t.Fatal(err)
		}
		nRead, err := f.ReadAt(buf[:5], 1024+offset)
		if err != nil && !logex.Equal(err, io.EOF) {
			logex.Error(err)
			t.Fatal(err)
		}
		if !bytes.Equal(buf[:nRead], buf2[:n]) {
			t.Fatal("result not expect")
		}
	}

	if _, err := f.ReadAt(buf, -1); err == nil {
		t.Fatal("expect error")
	}
}

func TestFileSize(t *testing.T) {
	dirname := utils.GetRoot("/test/bitmap.file.size")
	os.RemoveAll(dirname)
	f, err := NewFile(dirname)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if f.Size() != 0 {
		t.Fatal(err, f.Size())
	}

	if _, err := f.WriteAt([]byte("hello"), 12); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Create(dirname + "/.index"); err != nil {
		t.Fatal(err)
	}
	if f.Size() != 5+12 {
		t.Fatal("file size not expected", f.Size())
	}
}
