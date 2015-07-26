package main

import (
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/chzyer/muxque/message"
	"github.com/chzyer/muxque/topic"
	"gopkg.in/logex.v1"
)

var (
	m           sync.RWMutex
	topics      = map[string]*topic.Ins{}
	topicConfig *topic.Config
)

func init() {
	topicConfig = new(topic.Config)
	topicConfig.ChunkBit = 22
	topicConfig.Root = "/data/message/http/topic"
	os.MkdirAll(topicConfig.Root, 0777)
	os.RemoveAll(topicConfig.Root)
}

func getTopic(name string) (t *topic.Ins, err error) {
	m.RLock()
	t, ok := topics[name]
	m.RUnlock()
	if ok {
		return t, nil
	}
	t, err = topic.New(name, topicConfig)
	if err != nil {
		return t, logex.Trace(err)
	}

	m.Lock()
	topics[name] = t
	m.Unlock()
	return t, nil
}

func pubHandler(w http.ResponseWriter, req *http.Request) {
	name := req.FormValue("topic")
	if name == "" {
		http.Error(w, "missing topic", 403)
		return
	}
	size, err := strconv.Atoi(req.FormValue("size"))
	if err != nil {
		http.Error(w, err.Error(), 403)
		return
	}

	var (
		msg    *message.Ins
		header message.Header
		msgs   = make([]*message.Ins, 0, size)
	)
	for !logex.Equal(err, io.EOF) {
		msg, err = message.ReadMessage(&header, req.Body, message.RF_DEFAULT)
		if err != nil {
			break
		}
		msgs = append(msgs, msg)
	}

	t, err := getTopic(name)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	t.PutSync(msgs)
	w.Write([]byte("hello"))
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/put", pubHandler)
	http.ListenAndServe(":8611", mux)
}
