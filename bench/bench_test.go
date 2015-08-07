package bench

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/chzyer/muxque/cc"
	"github.com/chzyer/muxque/prot/message"
)

func BenchmarkHttpPut(b *testing.B) {
	var data []byte
	batch := 200
	for i := 0; i < batch; i++ {
		msg := message.NewByData(message.NewData([]byte(cc.RandString(200))))
		data = append(data, msg.Bytes()...)
	}
	r := bytes.NewReader(data)
	client := &http.Client{}
	buffer := 0
	url := "http://localhost:8611/put?topic=http-test&size=200"
	for i := 0; i < b.N; i++ {
		if buffer < batch {
			buffer++
			continue
		}
		buffer = 0
		r.Seek(0, 0)
		resp, err := client.Post(url, "", r)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
}
