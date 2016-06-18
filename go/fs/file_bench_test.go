package fs

import (
	"testing"

	"github.com/chzyer/test"
)

func BenchmarkFile1028(b *testing.B) {
	benchFile(b, 1028)
}

func nBenchmarkFile102800(b *testing.B) {
	benchFile(b, 102800)
}

func benchFile(b *testing.B, size int) {
	defer test.New(b)

	// fd, err := bio.NewFile(test.Root())
	// test.Nil(err)
	f := testNewFile(test.NewMemDisk())
	defer f.Close()

	data := test.RandBytes(size)

	for i := 0; i < b.N; i++ {
		_, err := f.Write(data)
		test.Nil(err)
		b.SetBytes(int64(size))
	}
	f.Sync()
}
