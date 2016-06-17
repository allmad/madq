package fs

import (
	"testing"

	"github.com/chzyer/madq/go/bio"
	"github.com/chzyer/test"
)

func BenchmarkFile(b *testing.B) {
	defer test.New(b)

	fd, err := bio.NewFile(test.Root())
	test.Nil(err)
	f := testNewFile(fd)
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
