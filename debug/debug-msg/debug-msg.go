package main

import (
	"encoding/binary"

	"github.com/chzyer/flagx"
	"github.com/chzyer/muxque/internal/utils"
	"github.com/chzyer/muxque/message"

	"gopkg.in/logex.v1"
)

type Config struct {
	MsgBin string
	MsgId  string
}

func NewConfig() *Config {
	var c Config
	flagx.Parse(&c)
	return &c
}

func msgBin(msgBytes []byte) {
	logex.Info("magic:", msgBytes[:message.SizeMsgMagic])
	logex.Info("length:", msgBytes[message.OffsetMsgLength:message.OffsetMsgLength+message.SizeMsgLength])
	logex.Info("msgid:", msgBytes[message.OffsetMsgId:message.OffsetMsgId+message.SizeMsgId])
	logex.Info("crc:", msgBytes[message.OffsetMsgCrc:message.OffsetMsgCrc+message.SizeMsgCrc])
	logex.Info("version:", msgBytes[message.OffsetMsgVer:message.OffsetMsgVer+message.SizeMsgVer])
	logex.Info("data:", msgBytes[message.OffsetMsgData:])
}

func msgId(msgid []byte) {
	logex.Info(binary.LittleEndian.Uint64(msgid))
}

func main() {
	c := NewConfig()
	if c.MsgBin != "" {
		msgBin(utils.ByteStr(c.MsgBin))
	}
	if c.MsgId != "" {
		msgId(utils.ByteStr(c.MsgId))
	}
}
