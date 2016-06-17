package fs

import (
	"testing"

	"github.com/chzyer/test"
)

func BenchmarkFile(b *testing.B) {
	defer test.New(b)

	f := testNewFile(test.NewMemDisk())
	defer f.Close()

	const size = 1028
	data := test.RandBytes(size)

	for i := 0; i < b.N; i++ {
		_, err := f.Write(data)
		test.Nil(err)
		b.SetBytes(size)
	}
	f.Sync()
}
