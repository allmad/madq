package mq

import (
	"net"
	"strings"

	"github.com/chzyer/muxque/topic"
	"gopkg.in/logex.v1"
)

func Listen(addr string, conf *topic.Config, runClient func(*Muxque, net.Conn)) (*Muxque, *net.TCPListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, logex.Trace(err)
	}
	que, err := NewMuxque(conf)
	if err != nil {
		return nil, nil, logex.Trace(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					logex.Error(err)
				}
				break
			}
			go runClient(que, conn)
		}
	}()
	return que, ln.(*net.TCPListener), nil
}
