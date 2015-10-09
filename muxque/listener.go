package mq

import (
	"net"
	"strings"

	"github.com/chzyer/fsmq/muxque/topic"

	"gopkg.in/logex.v1"
)

func Listen(addr string, conf *topic.Config, runClient func(*Muxque, *net.TCPConn)) (*Muxque, *net.TCPListener, error) {
	gln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, logex.Trace(err)
	}
	ln := gln.(*net.TCPListener)
	que, err := NewMuxque(conf)
	if err != nil {
		return nil, nil, logex.Trace(err)
	}
	go func() {
		for {
			conn, err := ln.AcceptTCP()
			if err != nil {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					logex.Error(err)
				}
				break
			}
			go runClient(que, conn)
		}
	}()
	return que, ln, nil
}
