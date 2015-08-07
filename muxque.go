package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/chzyer/muxque/muxque"
	"github.com/chzyer/muxque/muxque/topic"
	"github.com/chzyer/muxque/utils"

	"gopkg.in/logex.v1"
)

func RunClient(que *mq.Muxque, conn *net.TCPConn) {
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
