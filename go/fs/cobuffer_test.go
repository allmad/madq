package fs

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chzyer/test"
)

func BenchmarkCobuffer(b *testing.B) {
	defer test.New(b)

	buf := NewCobuffer(16<<20, 16<<20)
	data := test.SeqBytes(200)

	n := int64(b.N)
	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		var buffer []byte
		for {
			_, ok := <-buf.IsFlush()
			if !ok {
				break
			}

			for {
				n := buf.GetData(buffer)
				if n > 0 {
					buffer = make([]byte, n)
				} else {
					break
				}
			}
		}
	}()

	for i := 0; i < 4; i++ {
		go func() {
			defer wg.Done()

			for i := 0; ; i++ {
				if atomic.AddInt64(&n, -1) < 0 {
					break
				}

				buf.WriteData(data)
				b.SetBytes(200)
			}
		}()
	}
	wg.Wait()
	buf.Close()

}
