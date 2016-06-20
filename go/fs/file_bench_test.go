package fs

import (
	"testing"

	"github.com/chzyer/test"
)

func BenchmarkFile1028b(b *testing.B) {
	benchFile(b, 1028)
}

func BenchmarkFile200b(b *testing.B) {
	benchFile(b, 200)
}

func BenchmarkFile10280b(b *testing.B) {
	benchFile(b, 10280)
}

func benchFile(b *testing.B, size int) {
	defer test.New(b)

	// test.MarkLine()
	// fd, err := bio.NewFile(test.Root())
	// test.Nil(err)
	fd := test.NewMemDisk()
	f := testNewFile(fd)
	defer f.Close()

	data := test.RandBytes(size)

	for i := 0; i < b.N; i++ {
		_, err := f.Write(data)
		test.Nil(err)
		b.SetBytes(int64(size))
	}
	f.Sync()
}
