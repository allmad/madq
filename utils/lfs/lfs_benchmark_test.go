package lfs

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/chzyer/muxque/utils"
)

func BenchmarkWrite(b *testing.B) {
	lfs, err := newIns()
	if err != nil {
		b.Fatal(err)
		return
	}
	defer lfs.Close()

	worker := 1
	ch := make(chan string, worker)
	for i := 0; i < worker; i++ {
		ch <- utils.RandString(6)
	}
	line := int64(b.N)
	var wg sync.WaitGroup
	wg.Add(worker)
	var m sync.Mutex
	for i := 0; i < worker; i++ {
		go func() {
			defer wg.Done()

			w, err := lfs.OpenWriter("/" + <-ch)
			if err != nil {
				b.Error(err)
				return
			}
			defer w.Close()

			buf := bytes.Repeat([]byte(utils.RandString(1)), 4096)
			for ; atomic.LoadInt64(&line) > 0; atomic.AddInt64(&line, -1) {
				if _, err = w.Write(buf); err != nil {
					b.Error(err)
					break
				}
				m.Lock()
				b.SetBytes(int64(len(buf)))
				m.Unlock()
			}

		}()
	}
	wg.Wait()

}
