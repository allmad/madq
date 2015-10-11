package block

import (
	"testing"

	"github.com/chzyer/fsmq/fsmq/util/qio"
	"github.com/chzyer/fsmq/fsmq/util/qrand"
	"github.com/chzyer/fsmq/fsmq/util/test"
)

var benchmarkSize = 4096

func BenchmarkBlockWrite(b *testing.B) {
	defer test.New(b)
	writeN(b.N, b)
}

func writeN(n int, b *testing.B) {
	ins, err := New(root, DefaultBit)
	test.Nil(err)
	test.Nil(ins.Delete(false))
	w := qio.NewWriter(ins, 0)
	buf := qrand.RandBytes(benchmarkSize)
	for i := 0; i < n; i++ {
		n, err := w.Write(buf)
		test.Nil(err)
		if b != nil {
			b.SetBytes(int64(n))
		}
	}
}

func BenchmarkBlockRead(b *testing.B) {
	defer test.New(b)
	writeN(b.N, nil)

	b.ResetTimer()
	ins, err := New(root, DefaultBit)
	test.Nil(err)
	w := qio.NewReader(ins, 0)
	buf := make([]byte, benchmarkSize)
	for i := 0; i < b.N; i++ {
		n, err := w.Read(buf)
		test.Nil(err)
		b.SetBytes(int64(n))
	}
}
