package mq

import (
	"net"

	"github.com/chzyer/muxque/topic"
	"gopkg.in/logex.v1"
)

func Listen(addr string, conf *topic.Config, runClient func(*Muxque, net.Conn)) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return logex.Trace(err)
	}
	que, err := NewMuxque(conf)
	if err != nil {
		return logex.Trace(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				logex.Error(err)
				break
			}
			go runClient(que, conn)
		}
	}()
	return nil
}
