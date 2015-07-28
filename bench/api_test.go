package bench

import (
	"sync"
	"testing"

	"github.com/chzyer/muxque/api"
	"github.com/chzyer/muxque/internal/utils"
	"github.com/chzyer/muxque/message"
)

func BenchmarkApiConcurrentPut100(b *testing.B) {
	var data []*message.Ins
	batch := 200
	for i := 0; i < batch; i++ {
		d := message.NewData([]byte(utils.RandString(200)))
		data = append(data, message.NewByData(d))
	}

	client, err := api.New(":12345")
	if err != nil {
		b.Fatal(err)
	}

	buffer := 0
	worker := 100
	ch := make(chan []*message.Ins, 4)
	var wg sync.WaitGroup
	wg.Add(worker)
	go func() {
		for i := 0; i < b.N; i++ {
			if buffer < batch {
				buffer++
				continue
			}
			buffer = 0
			ch <- data
		}
		close(ch)
	}()
	for i := 0; i < worker; i++ {
		go func() {
			defer wg.Done()
			for data = range ch {
				_, err := client.Put("api-test", data)
				if err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func BenchmarkApiSyncPut(b *testing.B) {
	var data []*message.Ins
	batch := 200
	for i := 0; i < batch; i++ {
		d := message.NewData([]byte(utils.RandString(200)))
		data = append(data, message.NewByData(d))
	}

	client, err := api.New(":12345")
	if err != nil {
		b.Fatal(err)
	}

	buffer := 0
	for i := 0; i < b.N; i++ {
		if buffer < batch {
			buffer++
			continue
		}
		buffer = 0
		_, err := client.Put("api-test", data)
		if err != nil {
			b.Error(err)
			return
		}
	}

}
