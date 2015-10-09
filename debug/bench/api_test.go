package bench

import (
	"testing"

	"github.com/chzyer/fsmq/api"
	"github.com/chzyer/fsmq/rpc/message"
	"github.com/chzyer/fsmq/utils"
)

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func BenchmarkApiSyncGet(b *testing.B) {
	topic := "api-put"
	apiSyncPut(topic, b)
	b.ResetTimer()
	client, err := api.New(":12345")
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	offset := int64(0)
	bench := 200
	total := b.N
	for total > 0 {
		read := min(total, bench)
		total -= read

		if err := client.Get(topic, offset, read); err != nil {
			b.Fatal(err)
		}
		for reply := range client.ReplyChan() {
			read -= len(reply.Msgs)
			if read == 0 {
				offset = reply.Offset
				break
			}
		}
	}
}

func BenchmarkApiSyncPut(b *testing.B) {
	apiSyncPut("api-put", b)
}

func apiSyncPut(topic string, b *testing.B) {
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
	defer client.Close()

	if b.N < batch {
		_, err := client.Put(topic, data[:b.N])
		if err != nil {
			b.Error(err)
		}
		return
	}
	for i := 0; i < b.N; i += batch {
		_, err := client.Put(topic, data)
		if err != nil {
			b.Error(err)
			return
		}
	}
}
