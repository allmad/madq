package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/chzyer/muxque/internal/utils"
	"github.com/chzyer/muxque/mq"
	"github.com/chzyer/muxque/topic"
	"gopkg.in/logex.v1"
)

func RunClient(que *mq.Muxque, conn net.Conn) {
	mq.NewClient(que, conn)
}

func main() {
	conf := &topic.Config{
		Root:     utils.GetRoot("/topics/"),
		ChunkBit: 22,
	}
	_, _, err := mq.Listen(":12345", conf, RunClient)
	if err != nil {
		logex.Fatal(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGHUP)
	<-c
}
