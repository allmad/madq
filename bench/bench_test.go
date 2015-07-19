package bench

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/chzyer/mmq/internal/utils"
	"github.com/chzyer/mmq/mmq"
)

func BenchmarkHttpPut(b *testing.B) {
	var data []byte
	for i := 0; i < 100; i++ {
		msg := mmq.NewMessageByData(mmq.NewMessageData([]byte(utils.RandString(256))))
		data = append(data, msg.Bytes()...)
	}
	r := bytes.NewReader(data)
	client := &http.Client{}
	buffer := 0
	url := "http://localhost:8611/put?topic=http-test&size=100"
	for i := 0; i < b.N; i++ {
		if buffer < 100 {
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
