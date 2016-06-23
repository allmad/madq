package fs

import (
	"testing"

	"github.com/chzyer/madq/go/bio"
	"github.com/chzyer/test"
)

func BenchmarkFile1028b(b *testing.B) {
	defer test.New(b)
	fd := test.NewMemDisk()
	benchFile(b, 1028, fd)
}

func BenchmarkFile200DW(b *testing.B) {
	defer test.New(b)
	fd, err := bio.NewFile(test.Root())
	test.Nil(err)

	benchFile(b, 200, fd)
}

func BenchmarkFile200MW(b *testing.B) {
	defer test.New(b)
	fd := test.NewMemDisk()

	benchFile(b, 200, fd)
}

func BenchmarkFile10280b(b *testing.B) {
	defer test.New(b)
	fd := test.NewMemDisk()
	benchFile(b, 10280, fd)
}

func benchFile(b *testing.B, size int, fd bio.ReadWriterAt) {
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
